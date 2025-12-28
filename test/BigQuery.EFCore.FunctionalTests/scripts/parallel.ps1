#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Run EF Core BigQuery functional tests in parallel using different BigQuery datasets.
.DESCRIPTION
    This script runs functional tests in parallel by spawning multiple dotnet test processes,
    each with a different BQ_EFCORE_TEST_CONN_STRING environment variable pointing to different
    datasets. This significantly reduces total test execution time since BigQuery operations are slow.
.PARAMETER ProjectId
    The Google Cloud Project ID to use for testing. Defaults to the value from BQ_EFCORE_TEST_CONN_STRING
    or "test-project" if not set.
.PARAMETER AuthMethod
    Authentication method: ApplicationDefaultCredentials or JsonCredentials.
    Defaults to ApplicationDefaultCredentials.
.PARAMETER Merge
    If true (default), merges all TRX result files into a single TestResults.trx file using dotnet-trx-merge.
    The tool will be installed automatically if not present.
.PARAMETER TestGroups
    Specify which test groups to run. Can be broad categories (Query, Migrations) or specific groups
    (Query.SqlQuery, Migrations.SqlGenerator). Default is All. Can specify multiple groups separated by comma.
    Run with -ListGroups to see all available groups.
.PARAMETER ListGroups
    List all available test groups and exit.
.PARAMETER MaxParallel
    Maximum number of parallel test jobs to run. Defaults to 5.
.EXAMPLE
    .\parallel.ps1 -ProjectId "my-project-id"
.EXAMPLE
    .\parallel.ps1 -ProjectId "my-project-id" -Merge $false
.EXAMPLE
    .\parallel.ps1 -TestGroups ModelBuilding,Query -MaxParallel 3
.EXAMPLE
    .\parallel.ps1 -TestGroups Query.SqlQuery,Migrations.SqlGenerator
.EXAMPLE
    .\parallel.ps1 -ListGroups
#>
param(
    [string]$ProjectId = $null,
    [string]$AuthMethod = "ApplicationDefaultCredentials",
    [bool]$Merge = $true,
    [string[]]$TestGroups = @("All"),
    [switch]$ListGroups,
    [int]$MaxParallel = 5,
    [switch]$GenerateDashboard
)
$ErrorActionPreference = "Stop"
if (-not $PSBoundParameters.ContainsKey('GenerateDashboard')) {
    $GenerateDashboard = $true
}

# Generate unique run ID for this test execution (used by Northwind tests for unique dataset names)
$runId = [DateTime]::UtcNow.ToString("yyyyMMdd_HHmmss")
$env:BQ_TEST_RUN_ID = $runId

# Track jobs for cleanup
$script:runningJobs = @()

function New-DatasetName {
    param([string]$GroupName)

    $slug = ($GroupName -replace "[^A-Za-z0-9]+", "_").ToLower().Trim("_")
    if ($slug.Length -gt 60) {
        $slug = $slug.Substring(0, 60)
    }
    return "test_$slug"
}

function Get-DynamicTestGroups {
    param([string]$TestProject)

    Write-Host "Discovering test groups via dotnet test -t --no-build..." -ForegroundColor Cyan
    $arguments = @($TestProject, "-t", "--no-build")
    Write-Host "  dotnet test $($arguments -join ' ')" -ForegroundColor DarkGray

    $output = dotnet test @arguments 2>&1

    $groups = $output |
        ForEach-Object {
            if ($_ -match "\b(?<name>[^\s]*\.[^\s]*\.[^\s]*)") {
                $name = $matches["name"]
                $name -replace "\.[^.]+$", ""
            }
        } |
        Where-Object {
            $_ -and $_ -like "Ivy.EntityFrameworkCore.BigQuery.*" -and
            $_ -notmatch "^\\d+\\.\\d+$" -and
            $_ -notmatch "^restore\\.\\.\\." -and
            $_ -notmatch "^[A-Za-z]:\\\\"
        } |
        Group-Object |
        Sort-Object Name

    if (-not $groups) {
        Write-Warning "No test groups discovered from dotnet test -t."
        return @()
    }

    $groupEntries = @()
    foreach ($group in $groups) {
        $dataset = New-DatasetName -GroupName $group.Name

        $groupEntries += @{
            Name = $group.Name
            Dataset = $dataset
            Filter = "FullyQualifiedName~$($group.Name)"
            Description = $group.Name
        }
    }

    Write-Host "Discovered $($groupEntries.Count) test group(s)." -ForegroundColor Green
    return $groupEntries
}

function Get-TrxTestCounts {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        return $null
    }

    try {
        $xml = [xml](Get-Content -LiteralPath $Path -Raw)
        $ns = $xml.DocumentElement.NamespaceURI

        if ([string]::IsNullOrEmpty($ns)) {
            $results = $xml.SelectNodes("//UnitTestResult")
        } else {
            $nsMgr = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
            $nsMgr.AddNamespace("t", $ns)
            $results = $xml.SelectNodes("//t:UnitTestResult", $nsMgr)
        }

        if (-not $results) {
            return $null
        }

        $total = $results.Count
        $passed = ($results | Where-Object { $_.Outcome -eq "Passed" }).Count
        $failed = ($results | Where-Object { $_.Outcome -eq "Failed" }).Count
        $skipped = $total - $passed - $failed

        return @{
            Total = $total
            Passed = $passed
            Failed = $failed
            Skipped = $skipped
        }
    } catch {
        return $null
    }
}
# Cleanup function to stop all running jobs
function Cleanup-Jobs {
    param([string]$Reason = "Script termination")
    if ($script:runningJobs.Count -gt 0) {
        Write-Host ""
        Write-Host "Cleaning up running test jobs ($Reason)..." -ForegroundColor Yellow
        foreach ($jobInfo in $script:runningJobs) {
            $job = $jobInfo.Job
            if ($job.State -eq 'Running') {
                Write-Host "  Stopping: $($jobInfo.Group.Name)" -ForegroundColor Gray
                Stop-Job -Job $job -ErrorAction SilentlyContinue
                Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
            }
        }
        Write-Host "Cleanup complete." -ForegroundColor Yellow
    }
}
# Register Ctrl+C handler
$null = Register-EngineEvent -SourceIdentifier PowerShell.Exiting -Action {
    Cleanup-Jobs -Reason "PowerShell exiting"
}
$null = Register-EngineEvent -SourceIdentifier Console_CancelKeyPress -SupportEvent -Action {
    Write-Host ""
    Write-Host "Caught interrupt signal (Ctrl+C)" -ForegroundColor Red
    Cleanup-Jobs -Reason "User interrupt"
    [System.Environment]::Exit(130)
}
# Determine project ID from environment or parameter
if ([string]::IsNullOrEmpty($ProjectId)) {
    $envConnString = [Environment]::GetEnvironmentVariable("BQ_EFCORE_TEST_CONN_STRING")
    if ($envConnString -match "ProjectId=([^;]+)") {
        $ProjectId = $matches[1]
    } else {
        $ProjectId = "test-project"
    }
}
Write-Host "╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║  BigQuery EF Core Parallel Test Runner                        ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""
Write-Host "Project ID:     $ProjectId" -ForegroundColor Yellow
Write-Host "Auth Method:    $AuthMethod" -ForegroundColor Yellow
Write-Host "Max Parallel:   $MaxParallel" -ForegroundColor Yellow
Write-Host "Merge Results:  $Merge" -ForegroundColor Yellow
Write-Host ""
# Discover test groups with unique dataset names (aligned with Test Explorer grouping)
$testProject = Join-Path $PSScriptRoot ".." "Ivy.EFCore.BigQuery.FunctionalTests.csproj"
$allTestGroups = Get-DynamicTestGroups -TestProject $testProject
# Handle -ListGroups flag
if ($ListGroups) {
    Write-Host "Available test groups:" -ForegroundColor Cyan
    Write-Host ""
    $allTestGroups | ForEach-Object {
        Write-Host "  $($_.Name)" -ForegroundColor Yellow
        Write-Host "    Dataset:     $($_.Dataset)" -ForegroundColor Gray
        Write-Host "    Description: $($_.Description)" -ForegroundColor Gray
        Write-Host ""
    }
    exit 0
}
# Filter test groups based on parameter
$selectedGroups = if ($TestGroups -contains "All") {
    $allTestGroups
} else {
    $allTestGroups | Where-Object {
        $groupName = $_.Name
        $matched = $false
        foreach ($pattern in $TestGroups) {
            # Exact match or StartsWith match
            if ($groupName -eq $pattern -or $groupName.StartsWith("$pattern.") -or "$pattern.*" -like $groupName) {
                $matched = $true
                break
            }
        }
        $matched
    }
}
if ($selectedGroups.Count -eq 0) {
    Write-Host "No test groups matched the filter: $($TestGroups -join ', ')" -ForegroundColor Red
    Write-Host ""
    Write-Host "Run with -ListGroups to see all available test groups:" -ForegroundColor Yellow
    Write-Host "  .\parallel.ps1 -ListGroups" -ForegroundColor Gray
    Write-Host ""
    exit 1
}
Write-Host "Selected $($selectedGroups.Count) test group(s):" -ForegroundColor Green
$selectedGroups | ForEach-Object {
    Write-Host "  • $($_.Name) - $($_.Description)" -ForegroundColor Gray
}
Write-Host ""
# Ensure TestResults directory exists
$testResultsDir = Join-Path $PSScriptRoot "TestResults"
if (Test-Path $testResultsDir) {
    Write-Host "Cleaning existing TestResults directory..." -ForegroundColor Yellow
    Remove-Item -Path $testResultsDir -Recurse -Force
}
New-Item -ItemType Directory -Path $testResultsDir -Force | Out-Null
# Install dotnet-trx-merge if merge is enabled
if ($Merge) {
    Write-Host "Checking for dotnet-trx-merge tool..." -ForegroundColor Cyan
    $toolList = dotnet tool list --global
    if ($toolList -notmatch "dotnet-trx-merge") {
        Write-Host "Installing dotnet-trx-merge..." -ForegroundColor Yellow
        dotnet tool install --global dotnet-trx-merge
    } else {
        Write-Host "dotnet-trx-merge is already installed" -ForegroundColor Green
    }
    Write-Host ""
}
# Test project path
$testProject = Join-Path $PSScriptRoot ".." "Ivy.EFCore.BigQuery.FunctionalTests.csproj"
# Start time
$startTime = Get-Date
$results = @()
$groupSummaries = @()
$jobProgress = @{}

function Update-ProgressBars {
    for ($i = 0; $i -lt $script:runningJobs.Count; $i++) {
        $jobInfo = $script:runningJobs[$i]
        $job = $jobInfo.Job

        if ($job.State -eq 'Running') {
            $output = Receive-Job -Job $job -Keep 2>&1 | Out-String

            # DEBUG: Save first job's output to file for inspection
            if ($i -eq 0 -and $output.Length -gt 100) {
                $output | Set-Content -Path (Join-Path $testResultsDir "debug_output.txt") -Force
            }

            # Try to get total test count from discovery output: "Discovering: MyTests (found X test cases)"
            if ($jobProgress[$i].Total -eq 0 -and $output -match 'found (\d+) test case') {
                $jobProgress[$i].Total = [int]$matches[1]
            }

            # Parse xUnit output - try multiple patterns
            $passed = ([regex]::Matches($output, '\[PASS\]', [System.Text.RegularExpressions.RegexOptions]::Multiline)).Count
            $failed = ([regex]::Matches($output, '\[FAIL\]', [System.Text.RegularExpressions.RegexOptions]::Multiline)).Count
            $skipped = ([regex]::Matches($output, '\[SKIP\]', [System.Text.RegularExpressions.RegexOptions]::Multiline)).Count

            $jobProgress[$i].Passed = $passed
            $jobProgress[$i].Failed = $failed
            $jobProgress[$i].Skipped = $skipped
            $completed = $passed + $failed + $skipped

            $elapsed = [int]((Get-Date) - $jobProgress[$i].StartTime).TotalSeconds
            $percent = if ($jobProgress[$i].Total -gt 0) { [int](($completed / $jobProgress[$i].Total) * 100) } else { -1 }
            Write-Progress -Id $i -Activity $jobInfo.Group.Name -Status "P:$passed F:$failed S:$skipped ($completed/$($jobProgress[$i].Total), $elapsed sec)" -PercentComplete $percent
        } elseif ($job.State -eq 'Completed' -and -not $jobProgress[$i].Completed) {
            $p = $jobProgress[$i]
            $jobProgress[$i].Completed = $true
            Write-Progress -Id $i -Activity $jobInfo.Group.Name -Status "Complete: P:$($p.Passed) F:$($p.Failed) S:$($p.Skipped)" -Completed
        }
    }
}

# Cleanup function for CTRL+C
function Cleanup-Jobs {
    if ($script:runningJobs.Count -gt 0) {
        Write-Host "`nCleaning up jobs..." -ForegroundColor Yellow
        foreach ($jobInfo in $script:runningJobs) {
            if ($jobInfo.Job.State -eq 'Running') {
                Stop-Job -Job $jobInfo.Job -ErrorAction SilentlyContinue
            }
            Remove-Job -Job $jobInfo.Job -Force -ErrorAction SilentlyContinue
        }
        Write-Host "All jobs stopped." -ForegroundColor Green
    }
}

# Run tests in parallel using jobs
Write-Host "Starting parallel test execution with live progress..." -ForegroundColor Cyan
Write-Host ""

try {
    $groupIndex = 0
    foreach ($group in $selectedGroups) {
    # Wait if we've hit the max parallel limit, updating progress bars while waiting
    $ourRunningJobs = $script:runningJobs | Where-Object { $_.Job.State -eq 'Running' }
    while ($ourRunningJobs.Count -ge $MaxParallel) {
        Update-ProgressBars
        Start-Sleep -Milliseconds 300
        $ourRunningJobs = $script:runningJobs | Where-Object { $_.Job.State -eq 'Running' }
    }

    $groupIndex++
    $trxPath = Join-Path $testResultsDir "$($group.Name).trx"

    # Initialize progress tracking
    $jobProgress[$groupIndex - 1] = @{
        Passed = 0
        Failed = 0
        Skipped = 0
        Total = 0
        StartTime = Get-Date
        Completed = $false
    }

    # Show initial progress bar
    Write-Progress -Id ($groupIndex - 1) -Activity $group.Name -Status "Starting..." -PercentComplete -1

    $job = Start-Job -Name $group.Name -ScriptBlock {
        param($ProjectId, $AuthMethod, $Dataset, $Filter, $TestProject, $TrxPath)
        $env:BQ_EFCORE_TEST_CONN_STRING = "AuthMethod=$AuthMethod;ProjectId=$ProjectId;DefaultDatasetId=$Dataset"
        $cmdArgs = @("test", $TestProject, "--filter", $Filter, "--logger", "trx;LogFileName=$TrxPath", "--verbosity", "normal")
        $output = dotnet @cmdArgs 2>&1
        return @{ Output = $output; ExitCode = $LASTEXITCODE; TrxPath = $TrxPath }
    } -ArgumentList $ProjectId, $AuthMethod, $group.Dataset, $group.Filter, $testProject, $trxPath

    $script:runningJobs += @{ Job = $job; Group = $group; TrxPath = $trxPath }
}

# Continue monitoring until all complete
$stillRunning = $script:runningJobs | Where-Object { $_.Job.State -eq 'Running' }
while ($stillRunning.Count -gt 0) {
    Update-ProgressBars
    Start-Sleep -Milliseconds 300
    $stillRunning = $script:runningJobs | Where-Object { $_.Job.State -eq 'Running' }
}

# Collect results from all jobs
# Write-Host ""
# Write-Host "Collecting results..." -ForegroundColor Cyan
for ($i = 0; $i -lt $script:runningJobs.Count; $i++) {
    $jobInfo = $script:runningJobs[$i]
    $job = $jobInfo.Job
    $group = $jobInfo.Group

    Write-Progress -Id $i -Activity $group.Name -Completed

    $result = Receive-Job -Job $job -Wait
    Remove-Job -Job $job

    $success = $result.ExitCode -eq 0
    # $status = if ($success) { "✓ PASSED" } else { "✗ FAILED" }
    # $color = if ($success) { "Green" } else { "Red" }

    # $progress = $jobProgress[$i]
    # Write-Host "[$($i+1)/$($script:runningJobs.Count)] $status - $($group.Name) (P:$($progress.Passed) F:$($progress.Failed) S:$($progress.Skipped))" -ForegroundColor $color

    $results += @{
        Group = $group.Name
        Success = $success
        ExitCode = $result.ExitCode
        TrxPath = $result.TrxPath
    }
}
# Clear the running jobs list after completion
$script:runningJobs = @()
# Calculate elapsed time
$endTime = Get-Date
$elapsed = $endTime - $startTime
Write-Host ""
Write-Host "════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "Test Execution Complete" -ForegroundColor Cyan
Write-Host "════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""
Write-Host "Total Time: $($elapsed.ToString('hh\:mm\:ss'))" -ForegroundColor Yellow
Write-Host ""
# Display summary
$groupFailed = ($results | Where-Object { -not $_.Success }).Count

$testTotals = @{
    Total = 0
    Passed = 0
    Failed = 0
    Skipped = 0
}
$missingTrx = @()

foreach ($result in $results) {
    $counts = Get-TrxTestCounts -Path $result.TrxPath
    if ($counts) {
        $testTotals.Total += $counts.Total
        $testTotals.Passed += $counts.Passed
        $testTotals.Failed += $counts.Failed
        $testTotals.Skipped += $counts.Skipped
        $groupSummaries += @{
            Name = $result.Group
            Passed = $counts.Passed
            Failed = $counts.Failed
            Skipped = $counts.Skipped
            Total = $counts.Total
            TrxPath = $result.TrxPath
        }
    } else {
        $missingTrx += $result.TrxPath
        $groupSummaries += @{
            Name = $result.Group
            Passed = $null
            Failed = $null
            Skipped = $null
            Total = $null
            TrxPath = $result.TrxPath
        }
    }
}

Write-Host "Summary (tests):" -ForegroundColor White
Write-Host "  Passed: $($testTotals.Passed)" -ForegroundColor Green
Write-Host "  Failed: $($testTotals.Failed)" -ForegroundColor $(if ($testTotals.Failed -gt 0) { "Red" } else { "Gray" })
Write-Host "  Skipped: $($testTotals.Skipped)" -ForegroundColor Gray
Write-Host "  Total: $($testTotals.Total)" -ForegroundColor White

if ($missingTrx.Count -gt 0) {
    Write-Host "  (Counts unavailable for: $($missingTrx -join ', '))" -ForegroundColor Yellow
}
Write-Host ""

# Persist summary history
$historyPath = Join-Path $testResultsDir "history.json"
$history = @()
if (Test-Path $historyPath) {
    try {
        $loaded = Get-Content -Path $historyPath -Raw | ConvertFrom-Json
        # Flatten if double-nested (from old bug)
        if ($loaded -is [Array] -and $loaded.Count -eq 1 -and $loaded[0] -is [Array]) {
            $history = @($loaded[0])
        } elseif ($loaded -is [Array]) {
            $history = @($loaded)
        } elseif ($null -ne $loaded) {
            # Single object - wrap in array
            $history = @($loaded)
        }
    } catch {
        $history = @()
    }
}

$historyEntry = @{
    Timestamp = (Get-Date).ToString("o")
    Totals = $testTotals
    Groups = $groupSummaries
    Files = $results | ForEach-Object { $_.TrxPath }
}
$history += $historyEntry
if ($history.Count -gt 20) {
    $history = $history[($history.Count - 20)..($history.Count - 1)]
}
# Save as array (no double-wrapping)
$jsonOutput = ConvertTo-Json -InputObject $history -Depth 6
$jsonOutput | Set-Content -Path $historyPath

$mergedTrxPath = $null
# Merge TRX files if enabled
if ($Merge -and $results.Count -gt 1) {
    Write-Host "Merging TRX files..." -ForegroundColor Cyan
    $mergedTrxPath = Join-Path $testResultsDir "TestResults.trx"
    $trxFiles = $results | ForEach-Object { $_.TrxPath }
    try {
        # Use dotnet-trx-merge to combine results
        $trxArgs = @("--output", $mergedTrxPath)
        foreach ($trx in $trxFiles) {
            $trxArgs += @("--file", $trx)
        }

        trx-merge @trxArgs
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✓ Merged results saved to: $mergedTrxPath" -ForegroundColor Green
        } else {
            Write-Host "✗ Failed to merge TRX files (exit code: $LASTEXITCODE)" -ForegroundColor Red
        }
    } catch {
        Write-Host "✗ Error merging TRX files: $_" -ForegroundColor Red
    }
    Write-Host ""
}
# Build HTML dashboard from TRX results
if ($GenerateDashboard) {
    $dashboardInput = $null
    if ($Merge -and $results.Count -gt 1 -and $mergedTrxPath -and (Test-Path $mergedTrxPath)) {
        $dashboardInput = $mergedTrxPath
    } elseif ($results.Count -gt 0) {
        $candidate = ($results | Select-Object -First 1).TrxPath
        if (Test-Path $candidate) {
            $dashboardInput = $candidate
        }
    }

    if ($dashboardInput) {
        Write-Host "Building dashboard..." -ForegroundColor Cyan
        $dashboardScript = Join-Path $PSScriptRoot "build-dashboard.js"
        $dashboardOutput = Join-Path $testResultsDir "dashboard.html"
        $nodeArgs = @(
            $dashboardScript
            "--file"
            $dashboardInput
            "--out"
            $dashboardOutput
            "--history"
            $historyPath
        )

        node @nodeArgs
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✓ Dashboard generated at: $dashboardOutput" -ForegroundColor Green
        } else {
            Write-Host "✗ Failed to generate dashboard (exit code: $LASTEXITCODE)" -ForegroundColor Red
        }
        Write-Host ""
    } else {
        Write-Host "No TRX results available to build dashboard." -ForegroundColor Yellow
        Write-Host ""
    }
}
# List all TRX files
Write-Host "Test result files:" -ForegroundColor White
Get-ChildItem -Path $testResultsDir -Filter "*.trx" | ForEach-Object {
    Write-Host "  • $($_.Name)" -ForegroundColor Gray
}
Write-Host ""
} catch {
    Write-Host "`nInterrupted!" -ForegroundColor Red
    throw
} finally {
    Cleanup-Jobs
}

# Cleanup: Unregister event handler
Unregister-Event -SourceIdentifier Console_CancelKeyPress -ErrorAction SilentlyContinue
Unregister-Event -SourceIdentifier PowerShell.Exiting -ErrorAction SilentlyContinue
# Exit with failure code if any tests failed
$anyFailures = ($testTotals.Failed -gt 0) -or ($groupFailed -gt 0)
if ($anyFailures) {
    Write-Host "Some tests failed ($($testTotals.Failed) failed of $($testTotals.Total)). See individual TRX files for details." -ForegroundColor Red
    exit 1
} else {
    Write-Host "All tests passed ($($testTotals.Passed)/$($testTotals.Total))." -ForegroundColor Green
    exit 0
}
