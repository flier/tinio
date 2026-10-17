#!/usr/bin/env pwsh
# Verify the Windows sandbox, on every volume that can host it.
# Usage: pwsh crates/tinio-sandbox-windows/scripts/verify-windows.ps1 [libtest args...]
#        pwsh crates/tinio-sandbox-windows/scripts/verify-windows.ps1 -AclCost [<dirs> <files>]
#
# THE GATE'S ARGUMENTS ARE LIBTEST'S, NOT CARGO'S: they are appended after `--` to
# `cargo test -p tinio-sandbox-windows`, so a test filter, `--nocapture` or
# `--test-threads` selects a leg and a cargo flag does not. `--features x` is
# passed straight through to the test binary and fails as an unrecognized option
# (exit 101) rather than selecting a feature leg; run the cargo command yourself
# for that.
#
# WHAT IT NEEDS. Windows with `icacls.exe` in the system directory, and a
# console: the signal relay's cases fail rather than skip where a console control
# event cannot be delivered, because that event is the only mechanism this
# platform has for a stop — a detached session reports the refusal instead of a
# green suite that proved nothing. A volume the machine does not have is skipped
# rather than failed (`Test-Path` decides, and the filesystem each leg really
# covered is printed), but a selection that covers no volume at all is refused,
# because a suite looping over an empty list passes by doing nothing.
#
# EXIT STATUS. The script is the gate, so every native command's status is
# checked. `$ErrorActionPreference` covers cmdlets; a native command's status
# arrives in `$LASTEXITCODE` and nobody reading it leaves a red suite behind a
# green exit code, which is the one failure a gate exists to prevent.
param(
    # A different job from the gate: measure what the ACL pass costs on a
    # synthetic corpus (design §6 and its Risks row). Opt-in, because it builds
    # and deletes a large tree under `$env:TEMP`.
    [switch] $AclCost,
    # Everything else: the corpus shape in `-AclCost` mode (two integers), the
    # libtest arguments in the gate's (see the usage line — they reach the test
    # binary, not cargo). One remaining-argument list rather than typed
    # parameters, because a typed positional parameter would swallow the gate's
    # first argument before this list ever saw it.
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $Rest = @()
)

$ErrorActionPreference = 'Stop'
if (-not $IsWindows) { [Console]::Error.WriteLine('verify-windows.ps1 needs Windows and PowerShell 7 (pwsh)'); exit 1 }

function Assert-Exit([string] $what, [int] $code) {
    if ($code -ne 0) {
        [Console]::Error.WriteLine("$what failed (exit $code)")
        exit $code
    }
}

# The corpus mode runs instead of the gate and reports its own two numbers; it
# is here rather than in a shell history because the next reader has to be able
# to repeat it (design §6), and because the check that makes it a measurement
# rather than a fiction — `icacls` reported success — belongs in code.
if ($AclCost) {
    # The corpus shape, defaulting to the one this phase measured: 200
    # directories of 1000 files, 200,200 objects.
    $Dirs = if ($Rest.Count -ge 1) { [int] $Rest[0] } else { 200 }
    $Files = if ($Rest.Count -ge 2) { [int] $Rest[1] } else { 1000 }
    if ($Dirs -lt 1 -or $Files -lt 1) {
        [Console]::Error.WriteLine('-AclCost needs a positive directory and file count')
        exit 1
    }
    $corpus = Join-Path $env:TEMP 'tinio-acl-cost'
    Remove-Item -Recurse -Force $corpus -ErrorAction SilentlyContinue
    $objects = $Dirs * ($Files + 1)
    Write-Host "corpus: $Dirs directories x $Files files under $corpus ($objects objects)"

    $build = Measure-Command {
        foreach ($d in 1..$Dirs) {
            $dir = Join-Path $corpus "d$d"
            New-Item -ItemType Directory -Force $dir | Out-Null
            foreach ($f in 1..$Files) { [System.IO.File]::Create((Join-Path $dir "f$f.bin")).Dispose() }
        }
    }
    $created = (Get-ChildItem -Recurse -Force -File $corpus | Measure-Object).Count
    if ($created -ne $Dirs * $Files) {
        [Console]::Error.WriteLine("the corpus is $created files, not $($Dirs * $Files); the timing would be of another tree")
        exit 1
    }
    Write-Host ("created in {0:N2} s" -f $build.TotalSeconds)

    # A SID the tree does not carry, so what is timed is the propagation to
    # every object rather than whose SID it is. `*<SID>` is `icacls`'s own
    # spelling for a literal SID.
    #
    # **The `(OI)(CI)` spec below is the inherited shape on purpose, and it is the
    # one thing here that is not the launcher's current rule.** Ruling 78
    # (2026-09-22) gave the launcher's own directory a non-inherited ACE and its
    # sibling DLLs one file-shaped invocation through a `*.dll` pattern, so this
    # mode measures the *inheritance*
    # mechanism's per-object cost — which is what the caller's exposures still pay
    # and what this calibration exists for (design §6 and its Risks row,
    # `README.md`'s operator note). A measurement of the launcher's own directory
    # is `src/backend/tests.rs`'s exposure set and `src/acl/grant.rs`'s sibling-batch
    # test, not this mode.
    $sid = 'S-1-15-2-1-2-3'
    $deep = Join-Path (Join-Path $corpus "d$Dirs") "f$Files.bin"

    $grant = Measure-Command {
        # Captured so the tool's own text does not land between the two numbers;
        # the status is what the assertion below reads.
        $out = & icacls $corpus /grant:r "*${sid}:(OI)(CI)M" /q
        $script:grantExit = $LASTEXITCODE
    }
    Assert-Exit "icacls /grant:r $sid" $script:grantExit
    # Success of the tool is not yet propagation: the ACE the design's §6
    # paragraph rests on is the inherited copy on a pre-existing descendant,
    # which is what makes the number a per-object cost rather than one write.
    $inherited = & icacls $deep
    Assert-Exit "icacls $deep" $LASTEXITCODE
    if (($inherited | Out-String) -notmatch [regex]::Escape($sid)) {
        [Console]::Error.WriteLine("the grant did not reach $deep, so nothing was measured")
        exit 1
    }

    $rollback = Measure-Command {
        $out = & icacls $corpus /remove:g "*${sid}" /q
        $script:removeExit = $LASTEXITCODE
    }
    Assert-Exit "icacls /remove:g $sid" $script:removeExit
    $after = & icacls $deep
    Assert-Exit "icacls $deep" $LASTEXITCODE
    if (($after | Out-String) -match [regex]::Escape($sid)) {
        [Console]::Error.WriteLine("the rollback left $sid on $deep, so the second number is not a removal")
        exit 1
    }

    Remove-Item -Recurse -Force $corpus
    $perObjectMs = (($grant.TotalSeconds + $rollback.TotalSeconds) * 1000) / $objects
    Write-Host ("grant:    {0:N2} s" -f $grant.TotalSeconds)
    Write-Host ("rollback: {0:N2} s" -f $rollback.TotalSeconds)
    Write-Host ("both passes: {0:N2} s for {1} objects ({2:N4} ms per object)" -f ($grant.TotalSeconds + $rollback.TotalSeconds), $objects, $perObjectMs)
    exit 0
}

# The gate's default, and it wins here over the harness's own fallback (the volume holding the system temp directory, `crates/tinio-sandbox-windows/src/testing.rs` — `tests/common/` was folded into that module by the 2026-10-01 placement cleanup): this script is what covers an NTFS and an ReFS volume where the machine has both, which one volume cannot, and a bare `cargo test` against the crate gets the harness's fallback instead.
$requested = if ($env:TINIO_SANDBOX_TEST_VOLUMES) { $env:TINIO_SANDBOX_TEST_VOLUMES } else { 'C:\;E:\' }
$present = @($requested -split ';' | Where-Object { $_ -and (Test-Path $_) })
if ($present.Count -eq 0) { [Console]::Error.WriteLine("none of the volumes in '$requested' exist"); exit 1 }
$env:TINIO_SANDBOX_TEST_VOLUMES = $present -join ';'

Write-Host "volumes under test: $env:TINIO_SANDBOX_TEST_VOLUMES"
$filesystems = @()
foreach ($volume in $present) {
    $letter = ([System.IO.Path]::GetPathRoot((Resolve-Path $volume).Path)) -replace '\\$', ''
    if ($letter -notmatch '^[A-Za-z]:$') {
        Write-Host "  $volume : not a drive letter, so its filesystem is not resolved here"
        continue
    }
    # NTFS and ReFS agree on every probed ACL case (design evidence table), so
    # the log has to say which filesystem each leg actually covered.
    $fs = (Get-Volume -DriveLetter $letter.Substring(0, 1) -ErrorAction Stop).FileSystemType
    if (-not $fs) { [Console]::Error.WriteLine("no volume answers for '$volume'"); exit 1 }
    $filesystems += $fs
    Write-Host "  $volume : $fs"
}
# Design §12 asks for one NTFS and one ReFS volume; a machine that has only one
# is a fact about the machine rather than a broken build, so this is reported
# and not failed — what the gate refuses is a run that covered nothing.
if (($filesystems | Sort-Object -Unique).Count -lt 2) {
    Write-Host "note: this run covers ($($filesystems -join ', ')) only; design §12 wants an NTFS and an ReFS volume where the machine has both"
}

cargo test -p tinio-sandbox-windows -- @Rest
Assert-Exit 'cargo test -p tinio-sandbox-windows' $LASTEXITCODE

# The e2e leg resolves the `tinio` binary from its own profile directory, so it
# has to be built first and by the same profile: `--test <name>` selects one test
# target and builds no binary, and a clean tree would fail the leg below with a
# missing binary rather than a test result. P1's script builds for the same
# reason.
cargo build -p tinio
Assert-Exit 'cargo build -p tinio' $LASTEXITCODE

cargo test -p tinio-server --test sandbox_e2e_windows
Assert-Exit 'cargo test -p tinio-server --test sandbox_e2e_windows' $LASTEXITCODE

# **The confined scenarios, and this is where their real gate lives.** The
# cucumber harness excludes `@confinement` by default — no CI budget covers the
# launcher's ACL grant (`crates/tinio-e2e/tests/cucumber.rs`'s module doc) — so
# the hosted workflows cannot select it and the operator's opt-in plus this
# script are what run it.
#
# **An explicit `--tags` REPLACES the harness's default filter** rather than
# narrowing it: the default exclusions arrive through `CUCUMBER_FILTER_TAGS`,
# written only when no `--tags` was given, so the string below is the whole
# selection and nothing the default excluded stays excluded. `@cli and @FR-036`
# is spelled out for that reason, and `TINIO_E2E_CONFINEMENT=1` is what arms the
# scenario rather than the tag alone.
#
# **The platform axis is re-stated in the runner's own spelling**: a scenario
# carrying platform tags runs where at least one of them matches, so this
# Windows host excludes the scenarios that carry platform tags and `@windows`
# is not one of them — a `@linux`/`@macos`/`@unix` scenario runs on a host whose
# steps this one does not have, which `fail_on_skipped()` would turn into a red
# gate rather than a skip.
#
# **`--concurrency 1` is measured, not tidiness.** Two launchers running at once
# write DACLs on the same program directory (`target\debug` here) and one of them
# blows the 30 s readiness bound, which reads as a random red: six identical runs
# of one command on this host measured 4.6 s, 8.7 s, 8.7 s, 18.1 s, 57.7 s and
# one killed at >120 s.
$env:TINIO_E2E_CONFINEMENT = '1'
cargo test -p tinio-e2e --test cucumber -- --tags '@cli and @FR-036 and not ((@linux or @macos or @windows or @unix) and not @windows)' --concurrency 1
Assert-Exit "cargo test -p tinio-e2e --test cucumber --tags '@cli and @FR-036 and not ((@linux or @macos or @windows or @unix) and not @windows)' --concurrency 1" $LASTEXITCODE

# **No server process outlives the legs, and the check is the gate rather than tidiness.** A
# finished run once left 19 `tinio server <root> --port 0` processes alive after their parent had
# died; they held `target\debug\tinio.exe` open, so the next `cargo build -p tinio` failed at the
# uplift step (`failed to remove ... target\debug\tinio.exe: access denied`) and every later
# measurement ran against a stale binary until a hash comparison caught it. The check lives here
# because libtest has no run-level teardown to hang it on. **Run this script on a host with no
# other `tinio` server running**: it asserts the whole process table, which is the plan's own
# shape (`plans/2026-09-25-sandbox-followups.md` §13).
$leftover = @(Get-Process -Name tinio -ErrorAction SilentlyContinue)
if ($leftover.Count -ne 0) {
    [Console]::Error.WriteLine(
        "$($leftover.Count) tinio process(es) outlived the legs (PIDs $($leftover.Id -join ', ')): " +
        "a leaked server holds the binary open and the next build fails at the uplift step")
    exit 1
}

Write-Host 'verify-windows.ps1: every leg passed'
