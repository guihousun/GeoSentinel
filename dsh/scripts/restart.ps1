param([int]$Port = 8510)
$ErrorActionPreference = 'Stop'
$runtimeRoot = Split-Path -Parent $PSScriptRoot
$listeners = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
foreach ($listener in $listeners) {
    $serverProcess = Get-CimInstance Win32_Process -Filter "ProcessId=$($listener.OwningProcess)"
    if (-not $serverProcess.CommandLine.Contains($runtimeRoot)) {
        throw "Port $Port belongs to a different application; choose another port."
    }
    Stop-Process -Id $listener.OwningProcess
}
$logRoot = Join-Path $runtimeRoot '.runtime'
New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
$started = Start-Process -FilePath (Get-Command node).Source -ArgumentList @('scripts/start.mjs','--port',"$Port",'--no-open') -WorkingDirectory $runtimeRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $logRoot 'server.log') -RedirectStandardError (Join-Path $logRoot 'server-error.log') -PassThru
Write-Output "GeoSentinel launcher PID: $($started.Id)"
