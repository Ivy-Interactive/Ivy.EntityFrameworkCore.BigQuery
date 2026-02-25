# Analyze TRX file for failed tests and extract error messages
param(
    [string]$TrxPath = "D:\Repos\Ivy.EntityFrameworkCore.NewTests\test\BigQuery.EFCore.FunctionalTests\scripts\TestResults\trx\TestResults.trx"
)

[xml]$trx = Get-Content $TrxPath

$ns = New-Object System.Xml.XmlNamespaceManager($trx.NameTable)
$ns.AddNamespace("t", "http://microsoft.com/schemas/VisualStudio/TeamTest/2010")

# Get all failed test results
$failedTests = $trx.SelectNodes("//t:UnitTestResult[@outcome='Failed']", $ns)

Write-Host "Total failed tests: $($failedTests.Count)" -ForegroundColor Red
Write-Host ""

# Extract error messages and group by pattern
$errorGroups = @{}

foreach ($test in $failedTests) {
    $testName = $test.testName
    $errorMessage = $test.Output.ErrorInfo.Message
    $stackTrace = $test.Output.ErrorInfo.StackTrace

    # Skip StdOut - it's too long
    # Extract the key error pattern (first line or first meaningful part)
    if ($errorMessage) {
        # Get first line or first 200 chars as the error key
        $errorKey = ($errorMessage -split "`n")[0]
        if ($errorKey.Length -gt 200) {
            $errorKey = $errorKey.Substring(0, 200)
        }

        if (-not $errorGroups.ContainsKey($errorKey)) {
            $errorGroups[$errorKey] = @{
                Count = 0
                Tests = @()
                FullMessage = $errorMessage
                StackTrace = $stackTrace
            }
        }
        $errorGroups[$errorKey].Count++
        if ($errorGroups[$errorKey].Tests.Count -lt 5) {
            $errorGroups[$errorKey].Tests += $testName
        }
    }
}

# Sort by count (most common errors first)
$sortedErrors = $errorGroups.GetEnumerator() | Sort-Object { $_.Value.Count } -Descending

Write-Host "=== ERROR PATTERNS (grouped by first line) ===" -ForegroundColor Yellow
Write-Host ""

$i = 1
foreach ($error in $sortedErrors) {
    Write-Host "--- Error Pattern #$i (Count: $($error.Value.Count)) ---" -ForegroundColor Cyan
    Write-Host "Key: $($error.Key)" -ForegroundColor White
    Write-Host ""
    Write-Host "Sample tests:" -ForegroundColor Gray
    foreach ($testName in $error.Value.Tests) {
        Write-Host "  - $testName"
    }
    Write-Host ""

    # Show truncated full message
    $fullMsg = $error.Value.FullMessage
    if ($fullMsg.Length -gt 500) {
        $fullMsg = $fullMsg.Substring(0, 500) + "..."
    }
    Write-Host "Full message (truncated):" -ForegroundColor Gray
    Write-Host $fullMsg
    Write-Host ""

    # Show truncated stack trace (just first few lines)
    if ($error.Value.StackTrace) {
        $stackLines = ($error.Value.StackTrace -split "`n") | Select-Object -First 5
        Write-Host "Stack trace (first 5 lines):" -ForegroundColor Gray
        foreach ($line in $stackLines) {
            Write-Host "  $line"
        }
    }
    Write-Host ""
    Write-Host ("=" * 80)
    Write-Host ""
    $i++
}

# Summary
Write-Host ""
Write-Host "=== SUMMARY ===" -ForegroundColor Yellow
Write-Host "Total failed tests: $($failedTests.Count)"
Write-Host "Unique error patterns: $($errorGroups.Count)"
Write-Host ""
Write-Host "Top 10 most common errors:" -ForegroundColor Cyan
$sortedErrors | Select-Object -First 10 | ForEach-Object {
    Write-Host "  $($_.Value.Count) tests: $($_.Key.Substring(0, [Math]::Min(100, $_.Key.Length)))..."
}
