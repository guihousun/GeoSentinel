param([ValidateRange(1,65535)][int]$Port = 8511, [switch]$CheckOnly)
$ErrorActionPreference = 'Stop'
# A deployment can be relocated to another volume while a directory junction keeps the
# old spelling valid (measured 2026-09-12: the project moved to E:\GeoSentinel\project
# with D:\GeoSentinel-DSH left as a junction). The running instance's argv keeps the
# spelling it was launched with, so ownership must be decided on the RESOLVED path —
# otherwise the preflight refuses to stop the platform's own service.
# `path-alias.ps1` owns that resolution (and is unit-tested from dsh/tests).
. (Join-Path $PSScriptRoot 'path-alias.ps1')
$runtimeRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$nodeExe = (Get-Command node.exe -ErrorAction Stop).Source
$stateText = & $nodeExe (Join-Path $PSScriptRoot 'restart-state.mjs')
if ($LASTEXITCODE -ne 0) { throw 'Published release preflight failed; service unchanged.' }
$releaseState = ($stateText -join "`n") | ConvertFrom-Json
$expectedRelease = Resolve-RealPath $releaseState.directory
$listeners = @(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)
$servers = @(); $parents = @()
foreach ($listener in $listeners) {
    $server = Get-CimInstance Win32_Process -Filter "ProcessId=$($listener.OwningProcess)"
    $expectedProfile = 'geosentinel-' + $releaseState.id
    $matchesRelease = $false
    if ($server) {
        if ($server.CommandLine.Contains($releaseState.directory)) { $matchesRelease = $true }
        else {
            $found = [regex]::Match($server.CommandLine, '([A-Za-z]:[\\/][^"]*?releases[\\/]versions[\\/]' + [regex]::Escape($releaseState.id) + ')')
            if ($found.Success) { $matchesRelease = (Resolve-RealPath $found.Groups[1].Value).StartsWith($expectedRelease, [StringComparison]::OrdinalIgnoreCase) }
        }
    }
    if (!$server -or $server.Name -ne 'node.exe' -or !$matchesRelease -or !$server.CommandLine.Contains($expectedProfile)) { throw "Port $Port belongs to a different service; nothing stopped." }
    $parent = Get-CimInstance Win32_Process -Filter "ProcessId=$($server.ParentProcessId)"
    if (!$parent -or $parent.Name -ne 'node.exe' -or $parent.CommandLine -notmatch '(scripts[\\/]start\.mjs|\.runtime[\\/]start-(published|transition)\.mjs)') { throw 'Unknown supervisor; nothing stopped.' }
    $servers += $server; $parents += $parent
}
Write-Output "Published release: $($releaseState.id); port: $Port"
if ($CheckOnly) { Write-Output 'Preflight passed; no processes stopped.'; return }
foreach ($parent in ($parents | Sort-Object ProcessId -Unique)) { Stop-Process -Id $parent.ProcessId -ErrorAction Stop }
foreach ($server in ($servers | Sort-Object ProcessId -Unique)) { Stop-Process -Id $server.ProcessId -ErrorAction SilentlyContinue }
for ($attempt=0; $attempt -lt 30; $attempt++) {
    if (!(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)) { break }
    Start-Sleep -Milliseconds 500
}
if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) { throw 'Port not released; refusing duplicate startup.' }
& $nodeExe (Join-Path $PSScriptRoot 'background-start.mjs') $Port
if ($LASTEXITCODE -ne 0) { throw 'Background launch failed.' }
for ($attempt=0; $attempt -lt 45; $attempt++) {
    Start-Sleep -Seconds 1
    try {
        $health = Invoke-RestMethod "http://127.0.0.1:$Port/geo/api/health" -TimeoutSec 2
        if ($health.status -eq 'ok' -and $health.service -eq 'geosentinel-dsh' -and $health.release -eq $releaseState.id -and !$health.preview) {
            Write-Output "Ready: http://127.0.0.1:$Port/geo/native/ ($($health.release))"; return
        }
    } catch {}
}
throw "Startup did not pass health check. Logs: $runtimeRoot\.runtime\server-error.log"
