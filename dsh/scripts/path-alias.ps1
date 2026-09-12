# Resolve a Windows path to its real location, following directory junctions (and
# symlinks) at EVERY level of the chain.
#
# Why this exists: a deployment can be relocated to another volume while a junction
# keeps the old spelling valid. The process that is already running keeps the spelling
# it was launched with, so comparing two paths as strings then says "different service"
# about the platform's own instance — measured 2026-09-12, when the deployment moved
# from D:\GeoSentinel-DSH to E:\GeoSentinel\project and `restart.ps1` refused to stop
# its own service. Resolving component by component (a junction can sit at any level:
# both `D:\GeoSentinel-DSH` and `…\dsh\.runtime` were junctions) makes the comparison
# depend on the real directory instead of its spelling.
function Resolve-RealPath([string]$Path) {
    $full = [IO.Path]::GetFullPath($Path)
    $root = [IO.Path]::GetPathRoot($full)
    $current = $root
    foreach ($part in $full.Substring($root.Length).Split([IO.Path]::DirectorySeparatorChar, [StringSplitOptions]::RemoveEmptyEntries)) {
        $current = Join-Path $current $part
        for ($depth = 0; $depth -lt 16; $depth++) {
            $item = Get-Item -LiteralPath $current -Force -ErrorAction SilentlyContinue
            if (!$item -or !$item.LinkType -or !$item.Target) { break }
            $target = @($item.Target)[0]
            $current = if ([IO.Path]::IsPathRooted($target)) { [IO.Path]::GetFullPath($target) } else { [IO.Path]::GetFullPath((Join-Path (Split-Path -Parent $current) $target)) }
        }
    }
    return $current
}
