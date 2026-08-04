#!/usr/bin/env pwsh
# One-shot deploy of local grok2api (fork) to vmiss without full Docker build on the box.
#
# Why this shape:
# - Local Windows has no Docker daemon.
# - vmiss is 1 vCPU / ~1 GiB RAM; full multi-stage image builds thrash swap.
# - Production compose uses ghcr.io/aliothmoon/grok2api:latest with pull_policy=always.
#
# Flow:
#   1) Cross-compile linux/amd64 binary + build frontend on this machine
#   2) scp payload to vmiss
#   3) docker cp into running container, commit as local image, recreate with --pull never
#   4) health-check + print VERSION
#
# Usage:
#   pwsh ./scripts/deploy-vmiss.ps1
#   pwsh ./scripts/deploy-vmiss.ps1 -SkipBuild        # reuse .deploy artifacts
#   pwsh ./scripts/deploy-vmiss.ps1 -SshHost vmiss
#   pwsh ./scripts/deploy-vmiss.ps1 -RemoteDir /root/docker/grok2api

[CmdletBinding()]
param(
    [string]$SshHost = "vmiss",
    [string]$RemoteDir = "/root/docker/grok2api",
    [string]$Image = "ghcr.io/aliothmoon/grok2api:latest",
    [string]$Container = "grok2api",
    [switch]$SkipBuild,
    [switch]$SkipFrontend,
    [int]$HealthTimeoutSec = 90
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Write-Step([string]$Message) {
    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Invoke-Ssh([string]$Command, [int]$TimeoutSec = 120) {
    $args = @(
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=15",
        "-o", "ServerAliveInterval=15",
        $SshHost,
        $Command
    )
    # ssh itself has no -t timeout flag portably; rely on remote timeouts where needed.
    & ssh @args
    if ($LASTEXITCODE -ne 0) {
        throw "ssh failed (exit $LASTEXITCODE): $Command"
    }
}

function Invoke-Scp([string[]]$Sources, [string]$Destination) {
    $args = @(
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=15"
    ) + $Sources + @("${SshHost}:${Destination}")
    & scp @args
    if ($LASTEXITCODE -ne 0) {
        throw "scp failed (exit $LASTEXITCODE) -> ${SshHost}:${Destination}"
    }
}

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$DeployDir = Join-Path $Root ".deploy"
$BinaryPath = Join-Path $DeployDir "grok2api"
$FrontendDist = Join-Path $Root "frontend\dist"
$VersionPath = Join-Path $Root "VERSION"
$Stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$RemoteStage = "/tmp/grok2api-deploy-$Stamp"

if (-not (Test-Path $VersionPath)) {
    throw "VERSION file missing at $VersionPath"
}
$Version = (Get-Content $VersionPath -Raw).Trim()
if ([string]::IsNullOrWhiteSpace($Version)) {
    throw "VERSION file is empty"
}

Write-Host "Deploy grok2api $Version -> $SshHost ($RemoteDir)" -ForegroundColor Green
Write-Host "Image=$Image Container=$Container"

New-Item -ItemType Directory -Force -Path $DeployDir | Out-Null

if (-not $SkipBuild) {
    Write-Step "Cross-compile linux/amd64 binary"
    Push-Location (Join-Path $Root "backend")
    try {
        $env:CGO_ENABLED = "0"
        $env:GOOS = "linux"
        $env:GOARCH = "amd64"
        & go build -buildvcs=false -trimpath -ldflags="-s -w" -o $BinaryPath ./cmd/grok2api
        if ($LASTEXITCODE -ne 0) { throw "go build failed" }
    }
    finally {
        Remove-Item Env:CGO_ENABLED -ErrorAction SilentlyContinue
        Remove-Item Env:GOOS -ErrorAction SilentlyContinue
        Remove-Item Env:GOARCH -ErrorAction SilentlyContinue
        Pop-Location
    }

    if (-not $SkipFrontend) {
        Write-Step "Build frontend"
        Push-Location (Join-Path $Root "frontend")
        try {
            if (-not (Test-Path "node_modules")) {
                & pnpm install --frozen-lockfile
                if ($LASTEXITCODE -ne 0) { throw "pnpm install failed" }
            }
            & pnpm build
            if ($LASTEXITCODE -ne 0) { throw "pnpm build failed" }
        }
        finally {
            Pop-Location
        }
    }
}

if (-not (Test-Path $BinaryPath)) {
    throw "Binary missing: $BinaryPath (run without -SkipBuild)"
}
if (-not (Test-Path $FrontendDist)) {
    throw "Frontend dist missing: $FrontendDist (run without -SkipFrontend/-SkipBuild)"
}

$BinarySize = (Get-Item $BinaryPath).Length
Write-Host ("Binary size: {0:N1} MiB" -f ($BinarySize / 1MB))

Write-Step "Package payload"
$PayloadDir = Join-Path $DeployDir "payload"
if (Test-Path $PayloadDir) {
    Remove-Item -Recurse -Force $PayloadDir
}
New-Item -ItemType Directory -Force -Path (Join-Path $PayloadDir "frontend") | Out-Null
Copy-Item $BinaryPath (Join-Path $PayloadDir "grok2api") -Force
Copy-Item $VersionPath (Join-Path $PayloadDir "VERSION") -Force
Copy-Item -Recurse $FrontendDist (Join-Path $PayloadDir "frontend\dist")
$TarPath = Join-Path $DeployDir "grok2api-deploy-$Stamp.tar.gz"

# Prefer tar.exe (Windows 10+); fall back to Compress-Archive only if needed.
Push-Location $PayloadDir
try {
    & tar -czf $TarPath grok2api VERSION frontend
    if ($LASTEXITCODE -ne 0) { throw "tar package failed" }
}
finally {
    Pop-Location
}
Write-Host ("Payload: {0:N1} MiB" -f ((Get-Item $TarPath).Length / 1MB))

Write-Step "Upload to ${SshHost}:${RemoteStage}"
Invoke-Ssh "mkdir -p '$RemoteStage' '$RemoteDir'"
Invoke-Scp @($TarPath) "$RemoteStage/payload.tar.gz"

Write-Step "Remote install (docker cp + commit + recreate)"
# Remote bash: keep config/data volumes; never docker pull during recreate.
$RemoteScript = @"
set -euo pipefail
STAGE='$RemoteStage'
REMOTE_DIR='$RemoteDir'
IMAGE='$Image'
CONTAINER='$Container'
VERSION='$Version'
STAMP='$Stamp'
HEALTH_TIMEOUT='$HealthTimeoutSec'

cd "`$STAGE"
tar -xzf payload.tar.gz
chmod 0755 grok2api
test -f grok2api
test -f VERSION
test -d frontend/dist
test "`$(cat VERSION)" = "`$VERSION"

if ! docker inspect "`$CONTAINER" >/dev/null 2>&1; then
  echo "container `$CONTAINER not found" >&2
  exit 1
fi

echo "[remote] copy binary/frontend/VERSION into running container"
docker cp grok2api "`$CONTAINER":/app/grok2api
docker cp VERSION "`$CONTAINER":/app/VERSION
# Replace frontend tree atomically-ish: remove old dist then copy new
docker exec "`$CONTAINER" sh -c 'rm -rf /app/frontend/dist && mkdir -p /app/frontend'
docker cp frontend/dist "`$CONTAINER":/app/frontend/dist
docker exec "`$CONTAINER" sh -c 'chmod 0755 /app/grok2api && chown -R root:root /app/grok2api /app/VERSION /app/frontend || true'

echo "[remote] verify in-container version"
docker exec "`$CONTAINER" cat /app/VERSION

echo "[remote] commit local image tag (avoid GHCR pull of stale latest)"
BACKUP_TAG="`${IMAGE%:*}:pre-deploy-`${STAMP}"
# Keep previous running image as rollback tag
PREV_ID=`$(docker inspect -f '{{.Image}}' "`$CONTAINER")
docker tag "`$PREV_ID" "`$BACKUP_TAG" || true
docker commit \
  --change 'CMD ["/app/grok2api","--config","/app/config.yaml","--listen","0.0.0.0:8000"]' \
  "`$CONTAINER" "`$IMAGE"
docker tag "`$IMAGE" "`${IMAGE%:*}:`${VERSION}"
docker tag "`$IMAGE" "`${IMAGE%:*}:deploy-`${STAMP}"

echo "[remote] recreate container from local image (no pull)"
cd "`$REMOTE_DIR"
# compose v2 supports --pull never; fall back to COMPOSE_PULL_POLICY
if docker compose version >/dev/null 2>&1; then
  if docker compose up -d --pull never --force-recreate "`$CONTAINER" 2>/dev/null; then
    true
  else
    # service name may differ from container_name; use project service
    COMPOSE_PULL_POLICY=never docker compose up -d --force-recreate
  fi
else
  COMPOSE_PULL_POLICY=never docker-compose up -d --force-recreate
fi

echo "[remote] wait for health"
ok=0
for i in `$(seq 1 "`$HEALTH_TIMEOUT"); do
  if docker exec "`$CONTAINER" wget -qO- http://127.0.0.1:8000/healthz 2>/dev/null | grep -q ok; then
    ok=1
    break
  fi
  # alpine wget alternative
  if docker exec "`$CONTAINER" sh -c 'wget -qO- http://127.0.0.1:8000/healthz 2>/dev/null || true' | grep -q '"ok":true'; then
    ok=1
    break
  fi
  sleep 1
done
if [ "`$ok" != 1 ]; then
  echo "health check failed; recent logs:" >&2
  docker logs --tail 80 "`$CONTAINER" >&2 || true
  exit 1
fi

echo "[remote] deployed VERSION=`$(docker exec "`$CONTAINER" cat /app/VERSION)"
echo "[remote] image=`$(docker inspect -f '{{.Config.Image}} {{.Image}}' "`$CONTAINER")"
echo "[remote] status=`$(docker inspect -f '{{.State.Status}} health={{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}}' "`$CONTAINER")"
echo "[remote] rollback tag=`$BACKUP_TAG"
rm -rf "`$STAGE"
"@

# Feed script via stdin to avoid quoting hell on Windows OpenSSH.
$RemoteScript | & ssh -o BatchMode=yes -o ConnectTimeout=15 $SshHost "bash -s"
if ($LASTEXITCODE -ne 0) {
    throw "remote deploy failed (exit $LASTEXITCODE)"
}

Write-Step "Done"
Write-Host "grok2api $Version is live on $SshHost" -ForegroundColor Green
Write-Host "Public host: https://g2a.alioq.top  (traefik)"
Write-Host "Rollback example:"
Write-Host ("  ssh {0} `"cd {1} && docker tag {2}:pre-deploy-{3} {2} && COMPOSE_PULL_POLICY=never docker compose up -d --force-recreate`"" -f $SshHost, $RemoteDir, ($Image -replace ':latest$',''), $Stamp)
