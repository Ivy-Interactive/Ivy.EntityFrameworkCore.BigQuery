<#
.SYNOPSIS
    Analyzes TRX test result files and generates an error report grouped by error type.

.DESCRIPTION
    Parses .trx (Visual Studio Test Results) files from EF Core BigQuery tests,
    extracts failed tests, groups identical errors together, and generates a report.

    KEY FEATURES:
    - Groups identical errors (normalizes GUIDs, line numbers, etc.)
    - Categorizes by exception type (System.InvalidOperationException, Assert.Equal, etc.)
    - Extracts SQL from BigQuery errors and displays separately
    - Shows affected test names for each error group
    - Sorts by frequency (most common errors first)

.PARAMETER TrxPath
    Path to TRX file. Default: $PSScriptRoot\TestResults\trx\TestResults.trx

.PARAMETER OutputPath
    Save report to file instead of console output.

.PARAMETER ShowStackTraces
    Include stack traces (first 15 lines) for each error group.

.PARAMETER TopN
    Limit to top N error groups. Default 0 = show all.

.PARAMETER Filter
    Filter to only errors containing this pattern (regex). Example: "could not be translated"

.PARAMETER ShowVariants
    When errors are grouped together, show up to 3 different original messages to see the variants.

.EXAMPLE
    .\Analyze-TestResults.ps1
    # Analyze default TRX, show all errors

.EXAMPLE
    .\Analyze-TestResults.ps1 -TopN 10
    # Show only top 10 most frequent error groups

.EXAMPLE
    .\Analyze-TestResults.ps1 -TopN 5 -ShowStackTraces
    # Top 5 errors with stack traces

.EXAMPLE
    .\Analyze-TestResults.ps1 -OutputPath "report.txt"
    # Save full report to file

.EXAMPLE
    .\Analyze-TestResults.ps1 -Filter "could not be translated"
    # Show only errors containing "could not be translated"

.EXAMPLE
    .\Analyze-TestResults.ps1 -Filter "BigQueryException" -ShowVariants
    # Show BigQuery errors with different message variants displayed

.EXAMPLE
    .\Analyze-TestResults.ps1 -Filter "Translation.*failed|could not be translated" -TopN 5
    # Regex filter for multiple patterns, show top 5 groups

.OUTPUTS
    Report format:

    SUMMARY
    ----------------------------------------
    Total Tests:   8724
    Passed:        7648 (87.7%)
    Failed:        617 (7.1%)
    Skipped:       459
    Error Groups:  191

    ERRORS BY CATEGORY
    ----------------------------------------
      System.InvalidOperationException: 279 tests (68 unique errors)
      Ivy.Data.BigQuery.BigQueryException: 114 tests (50 unique errors)
      Assert.Equal: 131 tests (47 unique errors)
      ...

    DETAILED ERROR GROUPS (sorted by frequency)
    ================================================================================

    [1] System.InvalidOperationException - 116 test(s)
    --------------------------------------------------------------------------------
    Error: <error message>

    Affected tests:
      - TestName1(async: True)
      - TestName2(async: False)
      ... and N more tests

    [2] Ivy.Data.BigQuery.BigQueryException - 8 test(s)
    --------------------------------------------------------------------------------
    Error: Operands of & cannot be literal NULL

    SQL:
      SELECT `w`.`Id`, `w`.`AmmunitionType`
      FROM `Weapons` AS `w`
      WHERE `w`.`AmmunitionType` & NULL > 0

    Affected tests:
      - Where_bitwise_and_nullable_enum_with_null_constant(async: False)
      ...

.NOTES
    - For BigQuery errors, SQL is automatically extracted and shown separately
    - Error messages are normalized for grouping (GUIDs replaced, line numbers removed)
    - Test names are shortened to method name only for readability
    - Use -TopN to focus on most impactful errors first
#>

param(
    [Parameter(Position = 0)]
    [string]$TrxPath = "$PSScriptRoot\TestResults\trx\TestResults.trx",

    [Parameter()]
    [string]$OutputPath,

    [Parameter()]
    [switch]$ShowStackTraces,

    [Parameter()]
    [int]$TopN = 0,

    [Parameter()]
    [string]$Filter,

    [Parameter()]
    [switch]$ShowVariants
)

# Normalize error messages for grouping (removes variable parts like IDs, timestamps, etc.)
function Get-NormalizedErrorMessage {
    param([string]$Message)

    if ([string]::IsNullOrWhiteSpace($Message)) {
        return "[No error message]"
    }

    $normalized = $Message.Trim()

    # Remove GUIDs
    $normalized = $normalized -replace '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', '{GUID}'

    # Remove specific numeric values that vary (but keep error codes)
    $normalized = $normalized -replace '(?<=\s|^)\d{5,}(?=\s|$|\.)', '{NUMBER}'

    # Remove line/column numbers from assertion messages
    $normalized = $normalized -replace 'at position \d+', 'at position {N}'
    $normalized = $normalized -replace 'line \d+', 'line {N}'
    $normalized = $normalized -replace 'column \d+', 'column {N}'

    # Normalize expected/actual values in assertions (keep structure, remove specific values for very long ones)
    # But keep short values as they're often meaningful

    # EF Core translation errors - normalize LINQ expressions
    $normalized = $normalized -replace "The LINQ expression '.*?' could not be translated", "The LINQ expression '{EXPR}' could not be translated"
    $normalized = $normalized -replace "Translation of member '.*?' on entity type '.*?'", "Translation of member '{MEMBER}' on entity type '{TYPE}'"
    $normalized = $normalized -replace "Translation of method '.*?' failed", "Translation of method '{METHOD}' failed"
    $normalized = $normalized -replace "could not be translated\. Additional information:.*$", "could not be translated. Additional information: {INFO}"

    # Normalize DbSet and parameter references
    $normalized = $normalized -replace 'DbSet<\w+>\(\)', 'DbSet<T>()'
    $normalized = $normalized -replace '__\w+_\d+', '{PARAM}'

    # Normalize entity/property names in common patterns
    $normalized = $normalized -replace "Navigation '.*?' doesn't point to", "Navigation '{NAV}' doesn't point to"
    $normalized = $normalized -replace "Include has been used on non entity queryable", "Include has been used on non entity queryable"
    $normalized = $normalized -replace "Property '.*?' is not defined for type '.*?'", "Property '{PROP}' is not defined for type '{TYPE}'"

    return $normalized
}

# Extract SQL from error message (BigQuery errors often contain the failing SQL)
function Extract-SqlFromError {
    param([string]$Message)

    if ([string]::IsNullOrWhiteSpace($Message)) {
        return $null
    }

    # Split at (Reason: which marks the end of BigQuery error details
    $parts = $Message -split '\(Reason:'
    $mainPart = $parts[0]

    # Look for SQL statement start
    $lines = $mainPart -split "`n"
    $sqlLines = [System.Collections.ArrayList]@()
    $inSql = $false
    $sqlKeywords = '^(SELECT|INSERT|UPDATE|DELETE|MERGE|WITH)\s'

    foreach ($line in $lines) {
        $trimmed = $line.Trim()

        if (-not $inSql) {
            # Check if this line starts a SQL statement
            if ($trimmed -match $sqlKeywords) {
                $inSql = $true
                [void]$sqlLines.Add($trimmed)
            }
        }
        else {
            # We're in SQL - continue until we hit a terminator
            # Lines that are SQL continuations contain backticks, SQL keywords, or operators
            if ($trimmed -match '`\w+`|^\s*(FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|ON|AND|OR|GROUP|ORDER|HAVING|LIMIT|OFFSET|AS|SET|VALUES|INTO)\s|^\)|\(SELECT') {
                [void]$sqlLines.Add($trimmed)
            }
            elseif ($trimmed -match '^[A-Z_]+\s+`|^\s+`') {
                # Continuation line with SQL
                [void]$sqlLines.Add($trimmed)
            }
            elseif ($trimmed -eq '') {
                # Empty line might be part of formatting, skip
                continue
            }
            else {
                # Likely hit end of SQL
                break
            }
        }
    }

    if ($sqlLines.Count -eq 0) {
        return $null
    }

    # Clean up the last line - remove trailing error text
    # Pattern: SQL ends, then ): or : followed by error message
    $lastLine = $sqlLines[$sqlLines.Count - 1]

    # Match: ends with ) followed by : and error text
    if ($lastLine -match '^(.+\))\s*:\s*[A-Z]') {
        $sqlLines[$sqlLines.Count - 1] = $Matches[1]
    }
    # Match: ends with backtick-quoted identifier followed by : and error text
    elseif ($lastLine -match '^(.+`)\s*:\s*[A-Z]') {
        $sqlLines[$sqlLines.Count - 1] = $Matches[1]
    }
    # Match: ends with number or keyword followed by : and error text
    elseif ($lastLine -match '^(.+(?:\d+|NULL|TRUE|FALSE))\s*:\s*[A-Z]') {
        $sqlLines[$sqlLines.Count - 1] = $Matches[1]
    }
    # Match: general pattern - anything followed by : and capitalized word (error start)
    elseif ($lastLine -match '^(.+?)\s*:\s*[A-Z][a-z]+') {
        $sqlLines[$sqlLines.Count - 1] = $Matches[1]
    }

    $sql = $sqlLines -join "`n"
    return $sql
}

# Extract just the error message without the SQL
function Get-ErrorWithoutSql {
    param([string]$Message)

    if ([string]::IsNullOrWhiteSpace($Message)) {
        return $Message
    }

    # For BigQuery errors, extract just the error description
    if ($Message -match 'BigQueryException\s*:\s*Query execution failed:\s*(.+?)(?:\s+at\s+\[\d+:\d+\])?\s*$' -or
        $Message -match 'BigQueryException\s*:\s*Query execution failed:\s*(.+?)(?:\s+at\s+\[\d+:\d+\])?\s*[\r\n]') {
        # First line contains the actual error
        $firstLine = ($Message -split "`n")[0]
        if ($firstLine -match 'Query execution failed:\s*(.+?)(?:\s+at\s+\[\d+:\d+\])?$') {
            return $Matches[1].Trim()
        }
    }

    # For other errors, return first line
    $firstLine = ($Message -split "`n")[0]
    if ($firstLine.Length -gt 200) {
        return $firstLine.Substring(0, 200) + "..."
    }
    return $firstLine
}

# Extract a short error type/category from the message
function Get-ErrorCategory {
    param([string]$Message)

    if ([string]::IsNullOrWhiteSpace($Message)) {
        return "Unknown"
    }

    # Extract exception type if present
    if ($Message -match '^([\w\.]+Exception)\s*:') {
        return $Matches[1]
    }

    if ($Message -match '^([\w\.]+Exception)\s*$') {
        return $Matches[1]
    }

    # Common assertion patterns
    if ($Message -match 'Assert\.(\w+)') {
        return "Assert.$($Matches[1])"
    }

    if ($Message -match '^Expected:') {
        return "Assertion Failure"
    }

    if ($Message -match 'not equal|are equal|should be|expected') {
        return "Assertion Failure"
    }

    # BigQuery specific
    if ($Message -match 'BigQuery|400 Bad Request|Syntax error') {
        return "BigQuery Error"
    }

    if ($Message -match 'Translation of member') {
        return "Translation Error"
    }

    if ($Message -match 'could not be translated') {
        return "Translation Error"
    }

    return "Other"
}

# Parse the TRX file
function Parse-TrxFile {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        throw "TRX file not found: $Path"
    }

    [xml]$trx = Get-Content $Path -Encoding UTF8

    # Handle namespace
    $ns = New-Object System.Xml.XmlNamespaceManager($trx.NameTable)
    $ns.AddNamespace("t", "http://microsoft.com/schemas/VisualStudio/TeamTest/2010")

    $results = @{
        Total = 0
        Passed = 0
        Failed = 0
        Skipped = 0
        FailedTests = @()
    }

    # Try both namespaced and non-namespaced queries
    $testResults = $trx.SelectNodes("//t:UnitTestResult", $ns)
    if ($testResults.Count -eq 0) {
        $testResults = $trx.SelectNodes("//UnitTestResult")
    }

    foreach ($result in $testResults) {
        $results.Total++
        $outcome = $result.outcome

        switch ($outcome) {
            "Passed" { $results.Passed++ }
            "Failed" {
                $results.Failed++

                $errorInfo = $result.SelectSingleNode("Output/ErrorInfo")
                if (-not $errorInfo) {
                    $errorInfo = $result.SelectSingleNode("t:Output/t:ErrorInfo", $ns)
                }

                $message = ""
                $stackTrace = ""

                if ($errorInfo) {
                    $messageNode = $errorInfo.SelectSingleNode("Message")
                    if (-not $messageNode) {
                        $messageNode = $errorInfo.SelectSingleNode("t:Message", $ns)
                    }
                    $message = if ($messageNode) { $messageNode.InnerText } else { "" }

                    $stackNode = $errorInfo.SelectSingleNode("StackTrace")
                    if (-not $stackNode) {
                        $stackNode = $errorInfo.SelectSingleNode("t:StackTrace", $ns)
                    }
                    $stackTrace = if ($stackNode) { $stackNode.InnerText } else { "" }
                }

                $results.FailedTests += [PSCustomObject]@{
                    TestName = $result.testName
                    Message = $message
                    StackTrace = $stackTrace
                    Duration = $result.duration
                    NormalizedMessage = Get-NormalizedErrorMessage $message
                    ErrorCategory = Get-ErrorCategory $message
                    Sql = Extract-SqlFromError $message
                    ShortError = Get-ErrorWithoutSql $message
                }
            }
            "NotExecuted" { $results.Skipped++ }
            "Inconclusive" { $results.Skipped++ }
            default { }
        }
    }

    return $results
}

# Group failed tests by normalized error message
function Group-FailedTests {
    param($FailedTests)

    $groups = @{}

    foreach ($test in $FailedTests) {
        $key = $test.NormalizedMessage

        if (-not $groups.ContainsKey($key)) {
            $groups[$key] = [PSCustomObject]@{
                NormalizedMessage = $key
                OriginalMessage = $test.Message
                ShortError = $test.ShortError
                Category = $test.ErrorCategory
                StackTrace = $test.StackTrace
                Sql = $test.Sql
                Tests = [System.Collections.ArrayList]@()
                UniqueMessages = [System.Collections.ArrayList]@()
            }
        }

        [void]$groups[$key].Tests.Add($test.TestName)

        # Collect unique original messages for showing variants
        $shortMsg = if ($test.Message.Length -gt 500) { $test.Message.Substring(0, 500) } else { $test.Message }
        if ($groups[$key].UniqueMessages.Count -lt 10 -and -not $groups[$key].UniqueMessages.Contains($shortMsg)) {
            [void]$groups[$key].UniqueMessages.Add($shortMsg)
        }
    }

    return $groups.Values | Sort-Object { $_.Tests.Count } -Descending
}

# Generate the report
function Generate-Report {
    param(
        $Results,
        $Groups,
        [switch]$ShowStackTraces,
        [int]$TopN,
        [switch]$ShowVariants,
        [string]$Filter
    )

    $sb = [System.Text.StringBuilder]::new()

    [void]$sb.AppendLine("=" * 80)
    [void]$sb.AppendLine("TEST RESULTS ANALYSIS REPORT")
    [void]$sb.AppendLine("=" * 80)
    [void]$sb.AppendLine()

    # Summary
    [void]$sb.AppendLine("SUMMARY")
    [void]$sb.AppendLine("-" * 40)
    [void]$sb.AppendLine("Total Tests:   $($Results.Total)")
    [void]$sb.AppendLine("Passed:        $($Results.Passed) ($([math]::Round($Results.Passed / [math]::Max($Results.Total, 1) * 100, 1))%)")
    [void]$sb.AppendLine("Failed:        $($Results.Failed) ($([math]::Round($Results.Failed / [math]::Max($Results.Total, 1) * 100, 1))%)")
    [void]$sb.AppendLine("Skipped:       $($Results.Skipped)")
    [void]$sb.AppendLine("Error Groups:  $($Groups.Count)")
    if ($Filter) {
        $filteredCount = ($Groups | ForEach-Object { $_.Tests.Count } | Measure-Object -Sum).Sum
        [void]$sb.AppendLine("Filter:        '$Filter' ($filteredCount matching tests)")
    }
    [void]$sb.AppendLine()

    if ($Results.Failed -eq 0) {
        [void]$sb.AppendLine("All tests passed!")
        return $sb.ToString()
    }

    # Category summary
    [void]$sb.AppendLine("ERRORS BY CATEGORY")
    [void]$sb.AppendLine("-" * 40)
    $categoryGroups = $Groups | Group-Object Category | Sort-Object Count -Descending
    foreach ($cat in $categoryGroups) {
        $totalInCategory = ($cat.Group | ForEach-Object { $_.Tests.Count } | Measure-Object -Sum).Sum
        [void]$sb.AppendLine("  $($cat.Name): $totalInCategory tests ($($cat.Count) unique errors)")
    }
    [void]$sb.AppendLine()

    # Detailed error groups
    [void]$sb.AppendLine("DETAILED ERROR GROUPS (sorted by frequency)")
    [void]$sb.AppendLine("=" * 80)

    $groupsToShow = if ($TopN -gt 0) { $Groups | Select-Object -First $TopN } else { $Groups }
    $groupNum = 0

    foreach ($group in $groupsToShow) {
        $groupNum++
        [void]$sb.AppendLine()
        [void]$sb.AppendLine("[$groupNum] $($group.Category) - $($group.Tests.Count) test(s)")
        [void]$sb.AppendLine("-" * 80)

        # Show the error message (short version if SQL was extracted, otherwise original)
        if ($group.Sql) {
            # We have SQL - show short error and SQL separately
            [void]$sb.AppendLine("Error: $($group.ShortError)")
            [void]$sb.AppendLine()
            [void]$sb.AppendLine("SQL:")
            $sqlLines = $group.Sql -split "`n"
            foreach ($sqlLine in $sqlLines) {
                [void]$sb.AppendLine("  $sqlLine")
            }
        }
        else {
            # No SQL - show original error message
            $msgLines = $group.OriginalMessage -split "`n"
            $firstLine = $msgLines[0]
            if ($firstLine.Length -gt 200) {
                $firstLine = $firstLine.Substring(0, 200) + "..."
            }
            [void]$sb.AppendLine("Error: $firstLine")

            if ($msgLines.Count -gt 1) {
                # Show a few more lines if the message is multi-line
                for ($i = 1; $i -lt [math]::Min($msgLines.Count, 5); $i++) {
                    $line = $msgLines[$i]
                    if ($line.Length -gt 200) {
                        $line = $line.Substring(0, 200) + "..."
                    }
                    [void]$sb.AppendLine("       $line")
                }
                if ($msgLines.Count -gt 5) {
                    [void]$sb.AppendLine("       ... ($($msgLines.Count - 5) more lines)")
                }
            }
        }

        # Show variants if requested and there are multiple unique messages
        if ($ShowVariants -and $group.UniqueMessages.Count -gt 1) {
            [void]$sb.AppendLine()
            [void]$sb.AppendLine("Message variants ($($group.UniqueMessages.Count) unique):")
            $variantsToShow = $group.UniqueMessages | Select-Object -First 3
            $variantNum = 0
            foreach ($variant in $variantsToShow) {
                $variantNum++
                $firstLine = ($variant -split "`n")[0]
                if ($firstLine.Length -gt 150) {
                    $firstLine = $firstLine.Substring(0, 150) + "..."
                }
                [void]$sb.AppendLine("  [$variantNum] $firstLine")
            }
            if ($group.UniqueMessages.Count -gt 3) {
                [void]$sb.AppendLine("  ... and $($group.UniqueMessages.Count - 3) more variants")
            }
        }

        [void]$sb.AppendLine()
        [void]$sb.AppendLine("Affected tests:")

        # Show tests (limit to 20 if there are many)
        $testsToShow = if ($group.Tests.Count -gt 20) {
            $group.Tests | Select-Object -First 20
        } else {
            $group.Tests
        }

        foreach ($testName in $testsToShow) {
            # Extract just the test method name for brevity
            $shortName = $testName
            if ($testName -match '\.([^.]+)$') {
                $shortName = $Matches[1]
            }
            if ($testName -match '\.([^.]+\([^)]*\))$') {
                $shortName = $Matches[1]
            }
            [void]$sb.AppendLine("  - $shortName")
        }

        if ($group.Tests.Count -gt 20) {
            [void]$sb.AppendLine("  ... and $($group.Tests.Count - 20) more tests")
        }

        if ($ShowStackTraces -and $group.StackTrace) {
            [void]$sb.AppendLine()
            [void]$sb.AppendLine("Stack trace:")
            $stackLines = $group.StackTrace -split "`n" | Select-Object -First 15
            foreach ($line in $stackLines) {
                [void]$sb.AppendLine("  $($line.Trim())")
            }
            if (($group.StackTrace -split "`n").Count -gt 15) {
                [void]$sb.AppendLine("  ... (truncated)")
            }
        }
    }

    if ($TopN -gt 0 -and $Groups.Count -gt $TopN) {
        [void]$sb.AppendLine()
        [void]$sb.AppendLine("... and $($Groups.Count - $TopN) more error groups (use -TopN 0 to see all)")
    }

    [void]$sb.AppendLine()
    [void]$sb.AppendLine("=" * 80)
    [void]$sb.AppendLine("END OF REPORT")
    [void]$sb.AppendLine("=" * 80)

    return $sb.ToString()
}

# Main execution
try {
    Write-Host "Analyzing TRX file: $TrxPath" -ForegroundColor Cyan
    Write-Host ""

    $results = Parse-TrxFile -Path $TrxPath

    # Apply filter if specified
    $failedTests = $results.FailedTests
    if ($Filter) {
        $failedTests = @($failedTests | Where-Object { $_.Message -match $Filter })
        Write-Host "Filter '$Filter': $($failedTests.Count) of $($results.Failed) failed tests match" -ForegroundColor Yellow
        Write-Host ""
    }

    $groups = Group-FailedTests -FailedTests $failedTests

    $report = Generate-Report -Results $results -Groups $groups -ShowStackTraces:$ShowStackTraces -TopN $TopN -ShowVariants:$ShowVariants -Filter $Filter

    if ($OutputPath) {
        $report | Out-File -FilePath $OutputPath -Encoding UTF8
        Write-Host "Report saved to: $OutputPath" -ForegroundColor Green
    } else {
        Write-Output $report
    }
}
catch {
    Write-Error "Error analyzing TRX file: $_"
    exit 1
}
