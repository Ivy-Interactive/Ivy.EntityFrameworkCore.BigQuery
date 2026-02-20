#!/usr/bin/env node
// Generates an HTML dashboard from one or more TRX files.
// Usage:
//   node build-dashboard.js --file TestResults\\trx\\TestResults.trx --history TestResults\\history.json --out TestResults\\reports\\dashboard.html
//   node build-dashboard.js --dir TestResults\\trx --history TestResults\\history.json --out TestResults\\reports\\dashboard.html

const fs = require("fs");
const path = require("path");
const { XMLParser } = require("fast-xml-parser");

function parseArgs(argv) {
    const args = { files: [], dir: null, out: "dashboard.html", history: null, hideChildren: false, compareRun: null };
    for (let i = 0; i < argv.length; i++) {
        const arg = argv[i];
        if ((arg === "--file" || arg === "--merged" || arg === "--in") && argv[i + 1]) {
            args.files.push(argv[++i]);
        } else if (arg === "--dir" && argv[i + 1]) {
            args.dir = argv[++i];
        } else if (arg === "--out" && argv[i + 1]) {
            args.out = argv[++i];
        } else if (arg === "--history" && argv[i + 1]) {
            args.history = argv[++i];
        } else if ((arg === "--compare-run" || arg === "--compareRun") && argv[i + 1]) {
            args.compareRun = argv[++i];
        } else if (arg === "--hide-children") {
            args.hideChildren = true;
        }
    }
    return args;
}

function findTrxFiles(dir) {
    if (!dir) return [];
    try {
        return fs.readdirSync(dir)
            .filter(f => f.toLowerCase().endsWith(".trx"))
            .map(f => path.join(dir, f));
    } catch {
        return [];
    }
}

function parseDuration(duration) {
    if (!duration) return 0;
    const parts = duration.split(":"); // hh:mm:ss.fffffff
    if (parts.length < 3) return 0;
    const [h, m, s] = parts;
    const seconds = parseFloat(s);
    return (Number(h) * 3600) + (Number(m) * 60) + (isNaN(seconds) ? 0 : seconds);
}

function normalizeArray(value) {
    if (!value) return [];
    return Array.isArray(value) ? value : [value];
}

function getParentGroup(className) {
    if (!className) return "Unknown";
    const segments = className.split(".");
    if (segments.length <= 1) return className;
    segments.pop();
    return segments.join(".");
}

function timestampToRunId(timestamp) {
    if (!timestamp) return null;
    // Handle both ISO format (T separator) and space separator
    const match = timestamp.match(/(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})/);
    if (!match) return null;
    return `${match[1]}${match[2]}${match[3]}_${match[4]}${match[5]}${match[6]}`;
}

function loadHistory(historyPath, compareRunId = null) {
    if (!historyPath) return null;
    try {
        if (!fs.existsSync(historyPath)) return null;
        const data = JSON.parse(fs.readFileSync(historyPath, "utf8"));
        if (!Array.isArray(data) || data.length < 2) return null;

        // If a specific run ID is requested, find that entry
        if (compareRunId) {
            for (const entry of data) {
                const entryRunId = timestampToRunId(entry.Timestamp);
                if (entryRunId === compareRunId) {
                    return entry;
                }
            }
            // Run ID not found, fall back to previous run
            console.warn(`Run ID "${compareRunId}" not found in history, using previous run`);
        }

        // Default: return the second-to-last entry (previous run)
        return data[data.length - 2];
    } catch {
        return null;
    }
}

function loadPreviousTests(historyPath, compareRunId = null) {
    if (!historyPath) return new Map();
    try {
        if (!fs.existsSync(historyPath)) return new Map();
        const data = JSON.parse(fs.readFileSync(historyPath, "utf8"));
        if (!Array.isArray(data) || data.length < 2) return new Map();

        let prev = null;

        // If a specific run ID is requested, find that entry
        if (compareRunId) {
            for (const entry of data) {
                const entryRunId = timestampToRunId(entry.Timestamp);
                if (entryRunId === compareRunId) {
                    prev = entry;
                    break;
                }
            }
        }

        // Default: use the second-to-last entry
        if (!prev) {
            prev = data[data.length - 2];
        }

        if (!prev || !Array.isArray(prev.Tests)) return new Map();
        const map = new Map();
        for (const t of prev.Tests) {
            map.set(t.name, t.status);
        }
        return map;
    } catch {
        return new Map();
    }
}

function updateHistoryWithTests(historyPath, tests) {
    if (!historyPath) return;
    try {
        if (!fs.existsSync(historyPath)) return;
        const data = JSON.parse(fs.readFileSync(historyPath, "utf8"));
        if (!Array.isArray(data) || data.length === 0) return;
        // Add tests to the last (current) entry
        const current = data[data.length - 1];
        current.Tests = tests.map(t => ({ name: t.fullTestName, status: t.status }));
        fs.writeFileSync(historyPath, JSON.stringify(data, null, 2), "utf8");
    } catch (err) {
        console.warn(`Failed to update history with tests: ${err.message}`);
    }
}

function escapeHtml(text) {
    if (!text) return '';
    return text.toString()
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

function summarizeTrx(file) {
    const xmlText = fs.readFileSync(file, "utf8");
    const parser = new XMLParser({
        ignoreAttributes: false,
        attributeNamePrefix: "",
        allowBooleanAttributes: true,
    });
    const xml = parser.parse(xmlText);

    const unitTests = normalizeArray(xml?.TestRun?.TestDefinitions?.UnitTest);
    const results = normalizeArray(xml?.TestRun?.Results?.UnitTestResult);

    const testMap = new Map();
    for (const test of unitTests) {
        const id = test.id || test.Id;
        const method = test.TestMethod || {};
        const className = method.className || method.ClassName || "Unknown";
        // Prefer the full test name (includes params like "async: True") over bare method name
        const fullName = test.name || test.Name || "";
        let name = method.name || method.Name || "Unnamed";
        // Extract the test name with parameters from the full name if available
        if (fullName && fullName.includes(className + ".")) {
            const afterClass = fullName.substring(fullName.indexOf(className + ".") + className.length + 1);
            if (afterClass) name = afterClass;
        }
        testMap.set(id, { className, name });
    }

    const records = [];
    for (const result of results) {
        const testId = result.testId || result.TestId || result.testid || result.testID;
        const meta = testMap.get(testId) || { className: "Unknown", name: result.testName || result.TestName || "Unnamed" };
        const outcome = (result.outcome || result.Outcome || "Unknown").toLowerCase();
        const durationSec = parseDuration(result.duration || result.Duration);

        let status = "Skipped";
        if (outcome === "passed") status = "Passed";
        else if (outcome === "failed") status = "Failed";

        // Extract error information for failed tests
        let errorMessage = '';
        let stackTrace = '';
        let output = '';

        if (result.Output) {
            const outputObj = result.Output;

            // Error info
            if (outputObj.ErrorInfo) {
                const errorInfo = outputObj.ErrorInfo;
                errorMessage = errorInfo.Message || '';
                stackTrace = errorInfo.StackTrace || '';
            }

            // Standard output
            if (outputObj.StdOut) {
                output = outputObj.StdOut;
            }
        }

        const fullTestName = `${meta.className}.${meta.name}`;

        records.push({
            className: meta.className,
            testName: meta.name,
            fullTestName: fullTestName,
            status,
            durationSec,
            errorMessage,
            stackTrace,
            output,
        });
    }

    // Extract start and finish times for wall-clock calculation
    const times = xml?.TestRun?.Times || {};
    const startTime = times.start || times.Start;
    const finishTime = times.finish || times.Finish;

    return { records, startTime, finishTime };
}

function aggregate(records) {
    const parents = new Map();

    for (const rec of records) {
        const parentName = getParentGroup(rec.className);
        const childName = rec.className;

        if (!parents.has(parentName)) {
            parents.set(parentName, {
                name: parentName,
                passed: 0,
                failed: 0,
                skipped: 0,
                total: 0,
                durationSec: 0,
                children: new Map(),
                tests: [],
            });
        }
        const parent = parents.get(parentName);

        if (!parent.children.has(childName)) {
            parent.children.set(childName, {
                name: childName,
                passed: 0,
                failed: 0,
                skipped: 0,
                total: 0,
                durationSec: 0,
                tests: [],
            });
        }
        const child = parent.children.get(childName);

        child[rec.status.toLowerCase()] += 1;
        child.total += 1;
        child.durationSec += rec.durationSec;
        child.tests.push(rec);

        parent[rec.status.toLowerCase()] += 1;
        parent.total += 1;
        parent.durationSec += rec.durationSec;
        parent.tests.push(rec);
    }

    return Array.from(parents.values()).map((p) => ({
        ...p,
        children: Array.from(p.children.values()).sort((a, b) => b.total - a.total),
    })).sort((a, b) => b.total - a.total);
}

function buildModel(files) {
    let earliestStart = null;
    let latestFinish = null;
    const records = files.flatMap((f) => {
        try {
            const result = summarizeTrx(f);

            // Track earliest start and latest finish for wall-clock time
            if (result.startTime) {
                const start = new Date(result.startTime);
                if (!earliestStart || start < earliestStart) {
                    earliestStart = start;
                }
            }
            if (result.finishTime) {
                const finish = new Date(result.finishTime);
                if (!latestFinish || finish > latestFinish) {
                    latestFinish = finish;
                }
            }

            return result.records.map((r) => ({ ...r, source: path.basename(f) }));
        } catch (err) {
            console.warn(`Failed to parse ${f}: ${err.message}`);
            return [];
        }
    });
    const parents = aggregate(records);

    // Calculate wall-clock duration instead of summing individual durations
    let wallClockSec = 0;
    if (earliestStart && latestFinish) {
        wallClockSec = (latestFinish - earliestStart) / 1000;
    }

    const totals = parents.reduce((acc, p) => {
        acc.passed += p.passed;
        acc.failed += p.failed;
        acc.skipped += p.skipped;
        acc.total += p.total;
        return acc;
    }, { passed: 0, failed: 0, skipped: 0, total: 0, durationSec: wallClockSec });

    return { parents, totals, files, testRunDate: earliestStart };
}

function renderHtml(model, previous = null, prevTests = new Map(), currentGitInfo = null, prevGitInfo = null, currentRunId = null, prevRunId = null) {
    const dataJson = JSON.stringify(model);
    const prevJson = previous ? JSON.stringify(previous) : "null";
    // Convert Map to object for JSON serialization
    const prevTestsObj = Object.fromEntries(prevTests);
    const prevTestsJson = JSON.stringify(prevTestsObj);
    const generatedAt = model.testRunDate
        ? new Date(model.testRunDate).toLocaleString('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        })
        : '';

    // Git commit display info
    const currentGitShort = currentGitInfo?.shortCommit || null;
    const prevGitShort = prevGitInfo?.shortCommit || null;

    const clientScript = [
        "const model = JSON.parse(document.getElementById('data').textContent);",
        "const previous = " + prevJson + ";",
        "const prevTests = " + prevTestsJson + ";",
        "const getTestDeltaType = (fullTestName, currentStatus) => {",
        "  const prevStatus = prevTests[fullTestName];",
        "  if (!prevStatus) return '';",
        "  if (prevStatus === currentStatus) return '';",
        "  if (currentStatus === 'Passed' && prevStatus !== 'Passed') return 'improved';",
        "  if (currentStatus === 'Failed' && prevStatus === 'Passed') return 'regressed';",
        "  if (currentStatus === 'Skipped' && prevStatus === 'Passed') return 'regressed';",
        "  if (currentStatus === 'Skipped' && prevStatus === 'Failed') return 'improved';",
        "  return 'changed';",
        "};",
        "const getTestDelta = (fullTestName, currentStatus) => {",
        "  const deltaType = getTestDeltaType(fullTestName, currentStatus);",
        "  if (!deltaType) return '';",
        "  const prevStatus = prevTests[fullTestName];",
        "  if (deltaType === 'improved') return '<span class=\"delta-indicator delta-improved\" title=\"Was ' + prevStatus + '\">Δ</span>';",
        "  if (deltaType === 'regressed') return '<span class=\"delta-indicator delta-regressed\" title=\"Was ' + prevStatus + '\">Δ</span>';",
        "  return '<span class=\"delta-indicator delta-changed\" title=\"Was ' + prevStatus + '\">Δ</span>';",
        "};",
        "const copyIcon = '<svg viewBox=\"0 0 24 24\" width=\"14\" height=\"14\"><path fill=\"currentColor\" d=\"M16 1H4c-1.1 0-2 .9-2 2v14h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z\"/></svg>';",
        "function copyToClipboard(text, btn) {",
        "  navigator.clipboard.writeText(text).then(() => {",
        "    btn.classList.add('copied');",
        "    setTimeout(() => btn.classList.remove('copied'), 1500);",
        "  });",
        "}",
        "const formatPct = (part, total) => total ? Math.round((part / total) * 100) : 0;",
        "const formatDelta = (value, isFailure) => {",
        "  if (value === 0) return '<span style=\"color: var(--muted)\">0</span>';",
        "  if (isFailure) {",
        "    if (value > 0) return '<span style=\"color: var(--fail)\">▲' + value + '</span>';",
        "    return '<span style=\"color: var(--pass)\">▼' + Math.abs(value) + '</span>';",
        "  } else {",
        "    if (value > 0) return '<span style=\"color: var(--pass)\">▲' + value + '</span>';",
        "    return '<span style=\"color: var(--fail)\">▼' + Math.abs(value) + '</span>';",
        "  }",
        "};",
        "const trimPrefix = (name) => {",
        "  const trimmed = name.replace(/^Ivy\\.EntityFrameworkCore\\.BigQuery\\.?/, '');",
        "  if (!trimmed) return name;",
        "  return trimmed;",
        "};",
        "const formatSec = (sec) => {",
        "  if (!sec) return '0s';",
        "  const h = Math.floor(sec / 3600);",
        "  const m = Math.floor((sec % 3600) / 60);",
        "  const s = Math.round(sec % 60);",
        "  const parts = [];",
        "  if (h) parts.push(h + 'h');",
        "  if (m) parts.push(m + 'm');",
        "  if (s || parts.length === 0) parts.push(s + 's');",
        "  return parts.join(' ');",
        "};",
        "const escapeHtml = (text) => {",
        "  if (!text) return '';",
        "  const div = document.createElement('div');",
        "  div.textContent = text;",
        "  return div.innerHTML;",
        "};",
        "function toggleTests(groupName) {",
        "  const safeId = groupName.replace(/[^a-zA-Z0-9]/g, '_');",
        "  const testsEl = document.getElementById('tests-' + safeId);",
        "  if (!testsEl) return;",
        "  const isCurrentlyHidden = testsEl.style.display === 'none';",
        "  // Close all other open test lists first",
        "  document.querySelectorAll('.tests-container').forEach(el => {",
        "    if (el !== testsEl) {",
        "      el.style.display = 'none';",
        "      // Clear any selected test details in those containers",
        "      const detailsPanel = el.querySelector('.test-details-panel');",
        "      if (detailsPanel) {",
        "        detailsPanel.innerHTML = '<div class=\"no-test-selected\">Click on a test to view details</div>';",
        "      }",
        "    }",
        "  });",
        "  // Toggle current one",
        "  testsEl.style.display = isCurrentlyHidden ? 'flex' : 'none';",
        "  // Clear details panel when opening",
        "  if (isCurrentlyHidden) {",
        "    const detailsPanel = testsEl.querySelector('.test-details-panel');",
        "    if (detailsPanel) {",
        "      detailsPanel.innerHTML = '<div class=\"no-test-selected\">Click on a test to view details</div>';",
        "    }",
        "  }",
        "}",
        "function showTestDetails(parentName, testIndex, containerId) {",
        "  const parent = model.parents.find(p => p.name === parentName);",
        "  if (!parent) return;",
        "  const test = parent.tests[testIndex];",
        "  if (!test) return;",
        "  // Find the details panel in the same container",
        "  const container = document.getElementById(containerId);",
        "  if (!container) return;",
        "  const detailsPanel = container.querySelector('.test-details-panel');",
        "  if (!detailsPanel) return;",
        "  // Mark selected test item",
        "  container.querySelectorAll('.test-item').forEach(item => item.classList.remove('selected'));",
        "  const selectedItem = container.querySelector('.test-item[data-testname=\"' + CSS.escape(test.fullTestName) + '\"]');",
        "  if (selectedItem) selectedItem.classList.add('selected');",
        "  const testNameId = 'copy-testname-' + containerId + '-' + testIndex;",
        "  const classNameId = 'copy-classname-' + containerId + '-' + testIndex;",
        "  const errorId = 'copy-error-' + containerId + '-' + testIndex;",
        "  const stackId = 'copy-stack-' + containerId + '-' + testIndex;",
        "  const outputId = 'copy-output-' + containerId + '-' + testIndex;",
        "  let html = '<div class=\"details-header\">';",
        "  html += '<div class=\"detail-header-row\"><h3 style=\"margin: 0; font-size: 16px; word-break: break-word;\">' + escapeHtml(test.testName) + '</h3>';",
        "  html += '<button class=\"copy-btn\" id=\"' + testNameId + '\" title=\"Copy test name\">' + copyIcon + '</button></div>';",
        "  html += '</div>';",
        "  html += '<div class=\"details-content\">';",
        "  html += '<div class=\"detail-row\"><span class=\"detail-label\">Status:</span><span class=\"status-badge status-' + test.status.toLowerCase() + '\">' + test.status + '</span></div>';",
        "  html += '<div class=\"detail-row\"><span class=\"detail-label\">Class:</span><span>' + escapeHtml(test.className) + '</span>';",
        "  html += '<button class=\"copy-btn\" id=\"' + classNameId + '\" title=\"Copy class name\">' + copyIcon + '</button></div>';",
        "  html += '<div class=\"detail-row\"><span class=\"detail-label\">Duration:</span><span>' + formatSec(test.durationSec) + '</span></div>';",
        "  if (test.errorMessage) {",
        "    html += '<div class=\"detail-section\"><div class=\"detail-label-row\"><span class=\"detail-label\">Error Message:</span>';",
        "    html += '<button class=\"copy-btn\" id=\"' + errorId + '\" title=\"Copy error message\">' + copyIcon + '</button></div>';",
        "    html += '<pre class=\"error-text\">' + escapeHtml(test.errorMessage) + '</pre></div>';",
        "  }",
        "  if (test.stackTrace) {",
        "    html += '<div class=\"detail-section\"><div class=\"detail-label-row\"><span class=\"detail-label\">Stack Trace:</span>';",
        "    html += '<button class=\"copy-btn\" id=\"' + stackId + '\" title=\"Copy stack trace\">' + copyIcon + '</button></div>';",
        "    html += '<pre class=\"stack-trace\">' + escapeHtml(test.stackTrace) + '</pre></div>';",
        "  }",
        "  if (test.output) {",
        "    html += '<div class=\"detail-section\"><div class=\"detail-label-row\"><span class=\"detail-label\">Output:</span>';",
        "    html += '<button class=\"copy-btn\" id=\"' + outputId + '\" title=\"Copy output\">' + copyIcon + '</button></div>';",
        "    html += '<pre class=\"output-text\">' + escapeHtml(test.output) + '</pre></div>';",
        "  }",
        "  html += '</div>';",
        "  detailsPanel.innerHTML = html;",
        "  // Attach copy handlers",
        "  document.getElementById(testNameId).onclick = function() { copyToClipboard(test.testName, this); };",
        "  document.getElementById(classNameId).onclick = function() { copyToClipboard(test.className, this); };",
        "  if (test.errorMessage) document.getElementById(errorId).onclick = function() { copyToClipboard(test.errorMessage, this); };",
        "  if (test.stackTrace) document.getElementById(stackId).onclick = function() { copyToClipboard(test.stackTrace, this); };",
        "  if (test.output) document.getElementById(outputId).onclick = function() { copyToClipboard(test.output, this); };",
        "}",
        "// Draggable divider functionality",
        "let activeDivider = null;",
        "let startX = 0;",
        "let startLeftWidth = 0;",
        "function initDividerDrag(e, divider) {",
        "  e.preventDefault();",
        "  activeDivider = divider;",
        "  const container = divider.parentElement;",
        "  const leftPanel = container.querySelector('.test-list-panel');",
        "  startX = e.clientX;",
        "  startLeftWidth = leftPanel.offsetWidth;",
        "  document.addEventListener('mousemove', doDividerDrag);",
        "  document.addEventListener('mouseup', stopDividerDrag);",
        "  document.body.style.cursor = 'col-resize';",
        "  document.body.style.userSelect = 'none';",
        "}",
        "function doDividerDrag(e) {",
        "  if (!activeDivider) return;",
        "  const container = activeDivider.parentElement;",
        "  const leftPanel = container.querySelector('.test-list-panel');",
        "  const containerWidth = container.offsetWidth;",
        "  const dx = e.clientX - startX;",
        "  let newLeftWidth = startLeftWidth + dx;",
        "  // Constrain between 20% and 80% of container",
        "  const minWidth = containerWidth * 0.2;",
        "  const maxWidth = containerWidth * 0.8;",
        "  newLeftWidth = Math.max(minWidth, Math.min(maxWidth, newLeftWidth));",
        "  const leftPct = (newLeftWidth / containerWidth) * 100;",
        "  leftPanel.style.width = leftPct + '%';",
        "}",
        "function stopDividerDrag() {",
        "  activeDivider = null;",
        "  document.removeEventListener('mousemove', doDividerDrag);",
        "  document.removeEventListener('mouseup', stopDividerDrag);",
        "  document.body.style.cursor = '';",
        "  document.body.style.userSelect = '';",
        "}",
        "function filterTests(containerId, filter) {",
        "  const container = document.getElementById(containerId);",
        "  if (!container) return;",
        "  const buttons = container.querySelectorAll('.filter-btn');",
        "  buttons.forEach(btn => btn.classList.remove('active'));",
        "  const activeBtn = container.querySelector('.filter-btn[data-filter=\"' + filter + '\"]');",
        "  if (activeBtn) activeBtn.classList.add('active');",
        "  const items = container.querySelectorAll('.test-item');",
        "  items.forEach(item => {",
        "    if (filter === 'all') { item.style.display = 'flex'; }",
        "    else if (filter === 'passed' && item.classList.contains('status-passed')) { item.style.display = 'flex'; }",
        "    else if (filter === 'failed' && item.classList.contains('status-failed')) { item.style.display = 'flex'; }",
        "    else if (filter === 'skipped' && item.classList.contains('status-skipped')) { item.style.display = 'flex'; }",
        "    else if (filter === 'new-failed' && item.dataset.delta === 'regressed') { item.style.display = 'flex'; }",
        "    else if (filter === 'new-passed' && item.dataset.delta === 'improved') { item.style.display = 'flex'; }",
        "    else { item.style.display = 'none'; }",
        "  });",
        "}",
        "// Build a map of test full names to their error messages for searching",
        "const testErrorMap = new Map();",
        "model.parents.forEach(p => {",
        "  p.tests.forEach(t => {",
        "    if (t.errorMessage) {",
        "      testErrorMap.set(t.fullTestName, t.errorMessage.toLowerCase());",
        "    }",
        "  });",
        "});",
        "let currentSearchQuery = '';",
        "function searchErrors(query) {",
        "  currentSearchQuery = query.toLowerCase().trim();",
        "  const statusEl = document.getElementById('search-status');",
        "  const cards = document.querySelectorAll('.card[data-group]');",
        "  ",
        "  if (!currentSearchQuery) {",
        "    // Clear search - show all",
        "    statusEl.textContent = '';",
        "    cards.forEach(card => {",
        "      card.classList.remove('search-hidden');",
        "      const countEl = card.querySelector('.group-match-count');",
        "      if (countEl) countEl.remove();",
        "      // Reset test item visibility",
        "      card.querySelectorAll('.test-item').forEach(item => item.style.display = 'flex');",
        "      // Reset child group visibility",
        "      card.querySelectorAll('.child').forEach(child => child.style.display = '');",
        "    });",
        "    return;",
        "  }",
        "  ",
        "  let totalMatches = 0;",
        "  let groupsWithMatches = 0;",
        "  ",
        "  cards.forEach(card => {",
        "    const groupName = card.dataset.group;",
        "    const parent = model.parents.find(p => p.name === groupName);",
        "    if (!parent) return;",
        "    ",
        "    // Find matching tests in this group",
        "    const matchingTests = new Set();",
        "    parent.tests.forEach(t => {",
        "      const errorMsg = testErrorMap.get(t.fullTestName);",
        "      if (errorMsg && errorMsg.includes(currentSearchQuery)) {",
        "        matchingTests.add(t.fullTestName);",
        "      }",
        "    });",
        "    ",
        "    // Update group visibility and match count",
        "    let countEl = card.querySelector('.group-match-count');",
        "    if (matchingTests.size === 0) {",
        "      card.classList.add('search-hidden');",
        "      if (countEl) countEl.remove();",
        "    } else {",
        "      card.classList.remove('search-hidden');",
        "      groupsWithMatches++;",
        "      totalMatches += matchingTests.size;",
        "      ",
        "      // Add or update match count badge",
        "      const titleEl = card.querySelector('.card-title');",
        "      if (!countEl && titleEl) {",
        "        countEl = document.createElement('span');",
        "        countEl.className = 'group-match-count';",
        "        titleEl.parentNode.insertBefore(countEl, titleEl.nextSibling);",
        "      }",
        "      if (countEl) countEl.textContent = '(' + matchingTests.size + ' match' + (matchingTests.size === 1 ? '' : 'es') + ')';",
        "      ",
        "      // Filter test items in all containers within this card",
        "      card.querySelectorAll('.test-item').forEach(item => {",
        "        const testName = item.dataset.testname;",
        "        item.style.display = matchingTests.has(testName) ? 'flex' : 'none';",
        "      });",
        "      ",
        "      // Hide child groups that have no matching tests",
        "      card.querySelectorAll('.child').forEach(childEl => {",
        "        const childName = childEl.dataset.child;",
        "        const childObj = parent.children.find(c => c.name === childName);",
        "        if (!childObj) return;",
        "        const hasMatchingTest = childObj.tests.some(t => matchingTests.has(t.fullTestName));",
        "        childEl.style.display = hasMatchingTest ? '' : 'none';",
        "      });",
        "    }",
        "  });",
        "  ",
        "  statusEl.innerHTML = 'Found <span class=\"match-count\">' + totalMatches + '</span> test' + (totalMatches === 1 ? '' : 's') + ' in ' + groupsWithMatches + ' group' + (groupsWithMatches === 1 ? '' : 's');",
        "}",
        "function clearSearch() {",
        "  document.getElementById('error-search').value = '';",
        "  searchErrors('');",
        "}",
        "const summaryEl = document.getElementById('summary');",
        "const prevTotals = previous ? (previous.Totals || {}) : {};",
        "const prevTotalPass = prevTotals.Passed || 0;",
        "const prevTotalFail = prevTotals.Failed || 0;",
        "const prevTotalSkip = prevTotals.Skipped || 0;",
        "const deltaTotalPass = model.totals.passed - prevTotalPass;",
        "const deltaTotalFail = model.totals.failed - prevTotalFail;",
        "const deltaTotalSkip = model.totals.skipped - prevTotalSkip;",
        "const baseTotalPassPct = formatPct(Math.min(model.totals.passed, prevTotalPass), model.totals.total);",
        "const baseTotalFailPct = formatPct(Math.min(model.totals.failed, prevTotalFail), model.totals.total);",
        "const baseTotalSkipPct = formatPct(Math.min(model.totals.skipped, prevTotalSkip), model.totals.total);",
        "const deltaTotalPassPct = formatPct(Math.max(0, deltaTotalPass), model.totals.total);",
        "const deltaTotalFailPct = formatPct(Math.max(0, deltaTotalFail), model.totals.total);",
        "const deltaTotalSkipPct = formatPct(Math.max(0, deltaTotalSkip), model.totals.total);",
        "const totalPassPct = formatPct(model.totals.passed, model.totals.total);",
        "const totalFailPct = formatPct(model.totals.failed, model.totals.total);",
        "const totalSkipPct = 100 - totalPassPct - totalFailPct;",
        "let totalBarHtml = '<div class=\"card\">';",
        "totalBarHtml += '<div class=\"card-header\">';",
        "totalBarHtml += '<div class=\"card-title\">Total</div>';",
        "totalBarHtml += '<div class=\"meta\">';",
        "totalBarHtml += '<span>Passed: ' + model.totals.passed + '</span>';",
        "totalBarHtml += '<span>Failed: ' + model.totals.failed + '</span>';",
        "totalBarHtml += '<span>Skipped: ' + model.totals.skipped + '</span>';",
        "totalBarHtml += '<span>Total: ' + model.totals.total + '</span>';",
        "totalBarHtml += '<span>Time: ' + formatSec(model.totals.durationSec) + '</span>';",
        "if (previous) {",
        "  totalBarHtml += '<span>' + formatDelta(deltaTotalPass, false) + ' / ' + formatDelta(deltaTotalFail, true) + '</span>';",
        "}",
        "totalBarHtml += '</div></div>';",
        "totalBarHtml += '<div class=\"bar\">';",
        "let tLeft = 0;",
        "if (baseTotalPassPct > 0) {",
        "  totalBarHtml += '<div class=\"seg bar-pass\" style=\"left:' + tLeft + '%; width:' + baseTotalPassPct + '%\"></div>';",
        "  tLeft += baseTotalPassPct;",
        "}",
        "if (deltaTotalPassPct > 0) {",
        "  totalBarHtml += '<div class=\"seg bar-pass-delta\" style=\"left:' + tLeft + '%; width:' + deltaTotalPassPct + '%\"></div>';",
        "  tLeft += deltaTotalPassPct;",
        "}",
        "if (baseTotalFailPct > 0) {",
        "  totalBarHtml += '<div class=\"seg bar-fail\" style=\"left:' + tLeft + '%; width:' + baseTotalFailPct + '%\"></div>';",
        "  tLeft += baseTotalFailPct;",
        "}",
        "if (deltaTotalFailPct > 0) {",
        "  totalBarHtml += '<div class=\"seg bar-fail-delta\" style=\"left:' + tLeft + '%; width:' + deltaTotalFailPct + '%\"></div>';",
        "  tLeft += deltaTotalFailPct;",
        "}",
        "if (baseTotalSkipPct > 0) {",
        "  totalBarHtml += '<div class=\"seg bar-skip\" style=\"left:' + tLeft + '%; width:' + baseTotalSkipPct + '%\"></div>';",
        "  tLeft += baseTotalSkipPct;",
        "}",
        "if (deltaTotalSkipPct > 0) {",
        "  totalBarHtml += '<div class=\"seg bar-skip-delta\" style=\"left:' + tLeft + '%; width:' + deltaTotalSkipPct + '%\"></div>';",
        "}",
        "totalBarHtml += '</div>';",
        "totalBarHtml += '<div class=\"meta\">' + totalPassPct + '% passed | ' + totalFailPct + '% failed | ' + totalSkipPct + '% skipped</div>';",
        "totalBarHtml += '</div>';",
        "summaryEl.innerHTML = totalBarHtml;",
        "const prevChildMap = new Map();",
        "const prevParentMap = new Map();",
        "if (previous && Array.isArray(previous.Groups)) {",
        "  previous.Groups.forEach(g => {",
        "    if (g && g.Name) {",
        "      prevChildMap.set(g.Name, g);",
        "      const pName = g.Name.split('.').slice(0, -1).join('.');",
        "      if (!prevParentMap.has(pName)) {",
        "        prevParentMap.set(pName, { passed: 0, failed: 0, skipped: 0, total: 0 });",
        "      }",
        "      const p = prevParentMap.get(pName);",
        "      p.passed += g.Passed || 0;",
        "      p.failed += g.Failed || 0;",
        "      p.skipped += g.Skipped || 0;",
        "      p.total += g.Total || 0;",
        "    }",
        "  });",
        "}",
        "const groupsEl = document.getElementById('groups');",
        "const hideChildren = " + (parseArgs(process.argv.slice(2)).hideChildren ? "true" : "false") + ";",
        "const groupsHtml = model.parents.map(parent => {",
        "  const prevParent = prevParentMap.get(parent.name);",
        "  const prevPPass = prevParent ? (prevParent.passed || 0) : 0;",
        "  const prevPFail = prevParent ? (prevParent.failed || 0) : 0;",
        "  const prevPSkip = prevParent ? (prevParent.skipped || 0) : 0;",
        "  ",
        "  const deltaPPass = parent.passed - prevPPass;",
        "  const deltaPFail = parent.failed - prevPFail;",
        "  const deltaPSkip = parent.skipped - prevPSkip;",
        "  ",
        "  const basePassPct = formatPct(Math.min(parent.passed, prevPPass), parent.total);",
        "  const baseFailPct = formatPct(Math.min(parent.failed, prevPFail), parent.total);",
        "  const baseSkipPct = formatPct(Math.min(parent.skipped, prevPSkip), parent.total);",
        "  ",
        "  const deltaPassPct = formatPct(Math.max(0, deltaPPass), parent.total);",
        "  const deltaFailPct = formatPct(Math.max(0, deltaPFail), parent.total);",
        "  const deltaSkipPct = formatPct(Math.max(0, deltaPSkip), parent.total);",
        "  ",
        "  const passPct = formatPct(parent.passed, parent.total);",
        "  const failPct = formatPct(parent.failed, parent.total);",
        "  const skipPct = 100 - passPct - failPct;",
        "  const deltaLabel = prevParent ? (formatDelta(deltaPPass, false) + ' / ' + formatDelta(deltaPFail, true)) : '';",
        "  const childResults = parent.children.map(child => {",
        "    let displayName = child.name.replace(/\\+/g, '.');",
        "    const parentName = trimPrefix(parent.name);",
        "    const prefix = parent.name + '.';",
        "    if (displayName.startsWith(prefix)) { displayName = displayName.slice(prefix.length); }",
        "    if (displayName.startsWith(parentName + '.')) { displayName = displayName.slice(parentName.length + 1); }",
        "    const prevChild = prevChildMap.get(child.name);",
        "    const prevPass = prevChild ? (prevChild.Passed || 0) : 0;",
        "    const prevFail = prevChild ? (prevChild.Failed || 0) : 0;",
        "    const prevSkip = prevChild ? (prevChild.Skipped || 0) : 0;",
        "    const prevTotal = prevChild ? (prevChild.Total || 0) : 0;",
        "    ",
        "    const deltaPass = child.passed - prevPass;",
        "    const deltaFail = child.failed - prevFail;",
        "    const deltaSkip = child.skipped - prevSkip;",
        "    ",
        "    const basePassPct = formatPct(Math.min(child.passed, prevPass), child.total);",
        "    const baseFailPct = formatPct(Math.min(child.failed, prevFail), child.total);",
        "    const baseSkipPct = formatPct(Math.min(child.skipped, prevSkip), child.total);",
        "    ",
        "    const deltaPassPct = formatPct(Math.max(0, deltaPass), child.total);",
        "    const deltaFailPct = formatPct(Math.max(0, deltaFail), child.total);",
        "    const deltaSkipPct = formatPct(Math.max(0, deltaSkip), child.total);",
        "    ",
        "    const safeChildId = child.name.replace(/[^a-zA-Z0-9]/g, '_');",
        "    const childContainerId = 'tests-' + safeChildId;",
        "    const childTestItemsHtml = child.tests.map((test, idx) => {",
        "      const statusClass = 'status-' + test.status.toLowerCase();",
        "      const childIdx = parent.tests.findIndex(t => t.fullTestName === test.fullTestName);",
        "      const deltaType = getTestDeltaType(test.fullTestName, test.status);",
        "      const deltaHtml = getTestDelta(test.fullTestName, test.status);",
        "      const deltaAttr = deltaType ? ' data-delta=\"' + deltaType + '\"' : '';",
        "      const testNameAttr = ' data-testname=\"' + escapeHtml(test.fullTestName) + '\"';",
        "      return '<div class=\"test-item ' + statusClass + '\"' + deltaAttr + testNameAttr + ' onclick=\"showTestDetails(\\'' + parent.name.replace(/'/g, '\\\\\\'') + '\\', ' + childIdx + ', \\'' + childContainerId + '\\')\">' +",
        "        '<span class=\"test-icon\"></span>' +",
        "        '<span class=\"test-name\">' + escapeHtml(test.testName) + '</span>' +",
        "        deltaHtml +",
        "        '<span class=\"test-duration\">' + formatSec(test.durationSec) + '</span>' +",
        "        '</div>';",
        "    }).join('');",
        "    ",
        "    let left = 0;",
        "    let html = '';",
        "    html += '<div class=\"child\" data-child=\"' + child.name + '\">';",
        "    html += '<div class=\"eye-icon\" onclick=\"toggleTests(\\'' + child.name.replace(/'/g, '\\\\\\'') + '\\')\"><svg viewBox=\"0 0 24 24\"><path d=\"M12 4.5C7 4.5 2.73 7.61 1 12c1.73 4.39 6 7.5 11 7.5s9.27-3.11 11-7.5c-1.73-4.39-6-7.5-11-7.5zM12 17c-2.76 0-5-2.24-5-5s2.24-5 5-5 5 2.24 5 5-2.24 5-5 5zm0-8c-1.66 0-3 1.34-3 3s1.34 3 3 3 3-1.34 3-3-1.34-3-3-3z\"/></svg></div>';",
        "    html += '<div class=\"title\">' + displayName + '</div>';",
        "    html += '<div class=\"bar\">';",
        "    ",
        "    if (basePassPct > 0) {",
        "      html += '<div class=\"seg bar-pass\" style=\"left:' + left + '%; width:' + basePassPct + '%\"></div>';",
        "      left += basePassPct;",
        "    }",
        "    if (deltaPassPct > 0) {",
        "      html += '<div class=\"seg bar-pass-delta\" style=\"left:' + left + '%; width:' + deltaPassPct + '%\"></div>';",
        "      left += deltaPassPct;",
        "    }",
        "    if (baseFailPct > 0) {",
        "      html += '<div class=\"seg bar-fail\" style=\"left:' + left + '%; width:' + baseFailPct + '%\"></div>';",
        "      left += baseFailPct;",
        "    }",
        "    if (deltaFailPct > 0) {",
        "      html += '<div class=\"seg bar-fail-delta\" style=\"left:' + left + '%; width:' + deltaFailPct + '%\"></div>';",
        "      left += deltaFailPct;",
        "    }",
        "    if (baseSkipPct > 0) {",
        "      html += '<div class=\"seg bar-skip\" style=\"left:' + left + '%; width:' + baseSkipPct + '%\"></div>';",
        "      left += baseSkipPct;",
        "    }",
        "    if (deltaSkipPct > 0) {",
        "      html += '<div class=\"seg bar-skip-delta\" style=\"left:' + left + '%; width:' + deltaSkipPct + '%\"></div>';",
        "    }",
        "    ",
        "    html += '</div>';",
        "    html += '<div class=\"meta\">';",
        "    html += '<span>' + child.passed + '/' + child.failed + '/' + child.skipped + ' (' + formatSec(child.durationSec) + ')</span>';",
        "    if (prevChild) {",
        "      html += '<span>' + formatDelta(deltaPass, false) + ' / ' + formatDelta(deltaFail, true) + '</span>';",
        "    }",
        "    html += '</div>';",
        "    html += '</div>';",
        "    var testsContainer = '<div class=\"tests-container\" id=\"' + childContainerId + '\" style=\"display: none;\">';",
        "    testsContainer += '<div class=\"test-list-panel\">';",
        "    testsContainer += '<div class=\"filter-buttons\">';",
        "    testsContainer += '<button class=\"filter-btn active\" data-filter=\"all\" onclick=\"filterTests(\\'' + childContainerId + '\\', \\'all\\')\">All</button>';",
        "    testsContainer += '<button class=\"filter-btn\" data-filter=\"passed\" onclick=\"filterTests(\\'' + childContainerId + '\\', \\'passed\\')\">Passed</button>';",
        "    testsContainer += '<button class=\"filter-btn\" data-filter=\"failed\" onclick=\"filterTests(\\'' + childContainerId + '\\', \\'failed\\')\">Failed</button>';",
        "    testsContainer += '<button class=\"filter-btn\" data-filter=\"skipped\" onclick=\"filterTests(\\'' + childContainerId + '\\', \\'skipped\\')\">Skipped</button>';",
        "    testsContainer += '<button class=\"filter-btn filter-btn-delta\" data-filter=\"new-failed\" onclick=\"filterTests(\\'' + childContainerId + '\\', \\'new-failed\\')\">New Failed</button>';",
        "    testsContainer += '<button class=\"filter-btn filter-btn-delta\" data-filter=\"new-passed\" onclick=\"filterTests(\\'' + childContainerId + '\\', \\'new-passed\\')\">New Passed</button>';",
        "    testsContainer += '</div>';",
        "    testsContainer += '<div class=\"test-items-list\">' + childTestItemsHtml + '</div>';",
        "    testsContainer += '</div>';",
        "    testsContainer += '<div class=\"split-divider\" onmousedown=\"initDividerDrag(event, this)\"></div>';",
        "    testsContainer += '<div class=\"test-details-panel\"><div class=\"no-test-selected\">Click on a test to view details</div></div>';",
        "    testsContainer += '</div>';",
        "    return { childDiv: html, testsContainer: testsContainer };",
        "  });",
        "  const childHtml = childResults.map(r => r.childDiv).join('');",
        "  const childTestsContainersHtml = childResults.map(r => r.testsContainer).join('');",
        "  ",
        "  const safeParentId = parent.name.replace(/[^a-zA-Z0-9]/g, '_');",
        "  const parentContainerId = 'tests-' + safeParentId;",
        "  const testsItemsHtml = parent.tests.map((test, idx) => {",
        "    const statusClass = 'status-' + test.status.toLowerCase();",
        "    const deltaType = getTestDeltaType(test.fullTestName, test.status);",
        "    const deltaHtml = getTestDelta(test.fullTestName, test.status);",
        "    const deltaAttr = deltaType ? ' data-delta=\"' + deltaType + '\"' : '';",
        "    const testNameAttr = ' data-testname=\"' + escapeHtml(test.fullTestName) + '\"';",
        "    return '<div class=\"test-item ' + statusClass + '\"' + deltaAttr + testNameAttr + ' onclick=\"showTestDetails(\\'' + parent.name.replace(/'/g, '\\\\\\'') + '\\', ' + idx + ', \\'' + parentContainerId + '\\')\">' +",
        "      '<span class=\"test-icon\"></span>' +",
        "      '<span class=\"test-name\">' + escapeHtml(test.testName) + '</span>' +",
        "      deltaHtml +",
        "      '<span class=\"test-duration\">' + formatSec(test.durationSec) + '</span>' +",
        "      '</div>';",
        "  }).join('');",
        "  ",
        "  let card = '';",
        "  card += '<div class=\"card\" data-group=\"' + parent.name + '\">';",
        "  card += '<div class=\"eye-icon\" onclick=\"toggleTests(\\'' + parent.name.replace(/'/g, '\\\\\\'') + '\\')\"><svg viewBox=\"0 0 24 24\"><path d=\"M12 4.5C7 4.5 2.73 7.61 1 12c1.73 4.39 6 7.5 11 7.5s9.27-3.11 11-7.5c-1.73-4.39-6-7.5-11-7.5zM12 17c-2.76 0-5-2.24-5-5s2.24-5 5-5 5 2.24 5 5-2.24 5-5 5zm0-8c-1.66 0-3 1.34-3 3s1.34 3 3 3 3-1.34 3-3-1.34-3-3-3z\"/></svg></div>';",
        "  card += '<div class=\"card-header\">';",
        "  card += '<div class=\"card-title\">' + trimPrefix(parent.name) + '</div>';",
        "  card += '<div class=\"meta\">';",
        "  card += '<span>Passed: ' + parent.passed + '</span>';",
        "  card += '<span>Failed: ' + parent.failed + '</span>';",
        "  card += '<span>Skipped: ' + parent.skipped + '</span>';",
        "  card += '<span>Total: ' + parent.total + '</span>';",
        "  card += '<span>Time: ' + formatSec(parent.durationSec) + '</span>';",
        "  if (deltaLabel) { card += '<span>' + deltaLabel + '</span>'; }",
        "  card += '</div></div>';",
        "  card += '<div class=\"bar\">';",
        "  let pLeft = 0;",
        "  if (basePassPct > 0) {",
        "    card += '<div class=\"seg bar-pass\" style=\"left:' + pLeft + '%; width:' + basePassPct + '%\"></div>';",
        "    pLeft += basePassPct;",
        "  }",
        "  if (deltaPassPct > 0) {",
        "    card += '<div class=\"seg bar-pass-delta\" style=\"left:' + pLeft + '%; width:' + deltaPassPct + '%\"></div>';",
        "    pLeft += deltaPassPct;",
        "  }",
        "  if (baseFailPct > 0) {",
        "    card += '<div class=\"seg bar-fail\" style=\"left:' + pLeft + '%; width:' + baseFailPct + '%\"></div>';",
        "    pLeft += baseFailPct;",
        "  }",
        "  if (deltaFailPct > 0) {",
        "    card += '<div class=\"seg bar-fail-delta\" style=\"left:' + pLeft + '%; width:' + deltaFailPct + '%\"></div>';",
        "    pLeft += deltaFailPct;",
        "  }",
        "  if (baseSkipPct > 0) {",
        "    card += '<div class=\"seg bar-skip\" style=\"left:' + pLeft + '%; width:' + baseSkipPct + '%\"></div>';",
        "    pLeft += baseSkipPct;",
        "  }",
        "  if (deltaSkipPct > 0) {",
        "    card += '<div class=\"seg bar-skip-delta\" style=\"left:' + pLeft + '%; width:' + deltaSkipPct + '%\"></div>';",
        "  }",
        "  card += '</div>';",
        "  card += '<div class=\"meta\">' + passPct + '% passed | ' + failPct + '% failed | ' + skipPct + '% skipped</div>';",
        "  if (!hideChildren) { card += '<div class=\"children\">' + childHtml + '</div>' + childTestsContainersHtml; }",
        "  card += '<div class=\"tests-container\" id=\"' + parentContainerId + '\" style=\"display: none;\">';",
        "  card += '<div class=\"test-list-panel\">';",
        "  card += '<div class=\"filter-buttons\">';",
        "  card += '<button class=\"filter-btn active\" data-filter=\"all\" onclick=\"filterTests(\\'' + parentContainerId + '\\', \\'all\\')\">All</button>';",
        "  card += '<button class=\"filter-btn\" data-filter=\"passed\" onclick=\"filterTests(\\'' + parentContainerId + '\\', \\'passed\\')\">Passed</button>';",
        "  card += '<button class=\"filter-btn\" data-filter=\"failed\" onclick=\"filterTests(\\'' + parentContainerId + '\\', \\'failed\\')\">Failed</button>';",
        "  card += '<button class=\"filter-btn\" data-filter=\"skipped\" onclick=\"filterTests(\\'' + parentContainerId + '\\', \\'skipped\\')\">Skipped</button>';",
        "  card += '<button class=\"filter-btn filter-btn-delta\" data-filter=\"new-failed\" onclick=\"filterTests(\\'' + parentContainerId + '\\', \\'new-failed\\')\">New Failed</button>';",
        "  card += '<button class=\"filter-btn filter-btn-delta\" data-filter=\"new-passed\" onclick=\"filterTests(\\'' + parentContainerId + '\\', \\'new-passed\\')\">New Passed</button>';",
        "  card += '</div>';",
        "  card += '<div class=\"test-items-list\">' + testsItemsHtml + '</div>';",
        "  card += '</div>';",
        "  card += '<div class=\"split-divider\" onmousedown=\"initDividerDrag(event, this)\"></div>';",
        "  card += '<div class=\"test-details-panel\"><div class=\"no-test-selected\">Click on a test to view details</div></div>';",
        "  card += '</div>';",
        "  card += '</div>';",
        "  return card;",
        "}).join('');",
        "groupsEl.innerHTML = groupsHtml;",
    ].join("\n");

    const htmlParts = [];
    htmlParts.push("<!DOCTYPE html>");
    htmlParts.push("<html lang=\"en\">");
    htmlParts.push("<head>");
    htmlParts.push("  <meta charset=\"UTF-8\" />");
    htmlParts.push("  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />");
    htmlParts.push("  <title>BigQuery EF Core Test Dashboard</title>");
    htmlParts.push("  <style>");
    htmlParts.push("    :root {");
    htmlParts.push("      --bg: #041209;");
    htmlParts.push("      --panel: #121a15;");
    htmlParts.push("      --muted: #94a3b8;");
    htmlParts.push("      --text: #e2e8f0;");
    htmlParts.push("      --accent: #22d3ee;");
    htmlParts.push("      --accent-2: #3b82f6;");
    htmlParts.push("      --pass: #00d18e;");
    htmlParts.push("      --fail: #ef4444;");
    htmlParts.push("      --skip: #a0a3a1;");
    htmlParts.push("      --border: #26262b;");
    htmlParts.push("      --shadow: 0 10px 40px rgba(0,0,0,0.35);");
    htmlParts.push("    }");
    htmlParts.push("    * { box-sizing: border-box; }");
    htmlParts.push("    body {");
    htmlParts.push("      margin: 0; padding: 24px;");
    htmlParts.push("      font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;");
    htmlParts.push("      background: var(--bg);");
    htmlParts.push("      color: var(--text);");
    htmlParts.push("    }");
    htmlParts.push("    h1 { margin: 0 0 6px; font-size: 28px; letter-spacing: -0.4px; font-weight: 500; }");
    htmlParts.push("    .header-container { display: flex; align-items: baseline; flex-wrap: wrap; gap: 8px; }");
    htmlParts.push("    .run-ids { display: inline-flex; align-items: center; gap: 8px; opacity: 0; transition: opacity 0.2s; margin-left: 12px; }");
    htmlParts.push("    .header-container:hover .run-ids { opacity: 1; }");
    htmlParts.push("    .run-id-item { display: inline-flex; align-items: center; gap: 4px; font-size: 12px; color: var(--muted); }");
    htmlParts.push("    .run-id-item .run-id-value { font-family: monospace; color: var(--accent-2); }");
    htmlParts.push("    .run-id-item.comparison .run-id-value { color: #a78bfa; }");
    htmlParts.push("    .run-id-copy { background: transparent; border: 1px solid var(--border); border-radius: 3px; padding: 2px 4px; cursor: pointer; color: var(--muted); transition: all 0.15s; display: inline-flex; align-items: center; }");
    htmlParts.push("    .run-id-copy:hover { border-color: var(--accent); color: var(--accent); }");
    htmlParts.push("    .run-id-copy.copied { border-color: var(--pass); color: var(--pass); }");
    htmlParts.push("    .search-container { display: inline-flex; align-items: center; gap: 8px; margin-left: 16px; opacity: 0; transition: opacity 0.2s; }");
    htmlParts.push("    .header-container:hover .search-container { opacity: 1; }");
    htmlParts.push("    .search-input { background: var(--panel); border: 1px solid var(--border); border-radius: 6px; padding: 8px 12px; color: var(--text); font-size: 14px; width: 300px; outline: none; transition: border-color 0.2s; }");
    htmlParts.push("    .search-input:focus { border-color: var(--accent); }");
    htmlParts.push("    .search-input::placeholder { color: var(--muted); }");
    htmlParts.push("    .search-clear { background: transparent; border: 1px solid var(--border); border-radius: 4px; padding: 6px 12px; color: var(--muted); font-size: 12px; cursor: pointer; transition: all 0.15s; }");
    htmlParts.push("    .search-clear:hover { border-color: var(--accent); color: var(--text); }");
    htmlParts.push("    .search-status { color: var(--muted); font-size: 13px; margin-left: 8px; }");
    htmlParts.push("    .search-status .match-count { color: var(--accent); font-weight: 600; }");
    htmlParts.push("    .group-match-count { font-size: 12px; color: var(--accent); margin-left: 8px; font-weight: 600; }");
    htmlParts.push("    .card.search-hidden { display: none; }");
    htmlParts.push("    .summary {");
    htmlParts.push("      display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));");
    htmlParts.push("      gap: 12px; margin-bottom: 16px;");
    htmlParts.push("    }");
    htmlParts.push("    .badge { padding: 12px 14px; background: var(--panel); border: 2px solid var(--border); border-radius: 12px; box-shadow: var(--shadow); }");
    htmlParts.push("    .badge .label { color: var(--muted); font-size: 13px; }");
    htmlParts.push("    .badge .value { font-size: 20px; font-weight: 700; margin-top: 4px; }");
    htmlParts.push("    .groups { display: flex; flex-direction: column; gap: 14px; }");
    htmlParts.push("    .card {");
    htmlParts.push("      background: var(--panel); position: relative;");
    htmlParts.push("      border: 2px solid var(--border); border-radius: 16px; padding: 16px; box-shadow: var(--shadow);");
    htmlParts.push("    }");
    htmlParts.push("    .card-header { display: flex; justify-content: space-between; align-items: baseline; gap: 12px; flex-wrap: wrap; }");
    htmlParts.push("    .card-title { font-weight: 700; }");
    htmlParts.push("    .bar { position: relative; width: 100%; height: 14px; border-radius: 999px; background: #0b1222; overflow: hidden; margin: 8px 0 6px; }");
    htmlParts.push("    .bar .seg { position: absolute; top: 0; height: 100%; }");
    htmlParts.push("    .bar-pass { background: var(--pass); }");
    htmlParts.push("    .bar-fail { background: var(--fail); }");
    htmlParts.push("    .bar-skip { background: var(--skip); }");
    htmlParts.push("    .bar-pass-delta { background: #00a86b; }");
    htmlParts.push("    .bar-fail-delta { background: #ff6b6b; }");
    htmlParts.push("    .bar-skip-delta { background: #6b6d6a; }");
    htmlParts.push("    .meta { color: var(--muted); font-size: 13px; display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }");
    htmlParts.push("    .children { margin-top: 10px; display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 10px; }");
    htmlParts.push("    .child { padding: 10px 12px; background: rgba(255,255,255,0.02); border: 1px solid var(--border); border-radius: 12px; position: relative; }");
    htmlParts.push("    .child .title { font-weight: 600; font-size: 14px; margin-bottom: 6px; color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }");
    htmlParts.push("    .legend { display: flex; gap: 12px; align-items: center; margin: 10px 0 18px; color: var(--muted); font-size: 13px; }");
    htmlParts.push("    .dot { width: 12px; height: 12px; border-radius: 999px; display: inline-block; }");
    htmlParts.push("    .eye-icon {");
    htmlParts.push("      position: absolute; top: 16px; right: 16px; width: 24px; height: 24px;");
    htmlParts.push("      cursor: pointer; opacity: 0; transition: opacity 0.2s, transform 0.2s;");
    htmlParts.push("      z-index: 10;");
    htmlParts.push("    }");
    htmlParts.push("    .child .eye-icon { top: 10px; right: 12px; width: 20px; height: 20px; }");
    htmlParts.push("    .card:hover .eye-icon, .child:hover .eye-icon { opacity: 0.6; }");
    htmlParts.push("    .eye-icon:hover { opacity: 1 !important; transform: scale(1.1); }");
    htmlParts.push("    .eye-icon svg { width: 100%; height: 100%; fill: var(--accent); }");
    htmlParts.push("    .tests-container {");
    htmlParts.push("      display: flex; flex-direction: row; margin-top: 14px; padding: 14px;");
    htmlParts.push("      border-top: 1px solid var(--border); height: 400px; min-height: 200px;");
    htmlParts.push("    }");
    htmlParts.push("    .test-list-panel {");
    htmlParts.push("      width: 40%; min-width: 150px; padding-right: 8px;");
    htmlParts.push("      display: flex; flex-direction: column;");
    htmlParts.push("    }");
    htmlParts.push("    .filter-buttons {");
    htmlParts.push("      display: flex; gap: 6px; margin-bottom: 10px; flex-shrink: 0;");
    htmlParts.push("    }");
    htmlParts.push("    .filter-btn {");
    htmlParts.push("      padding: 4px 10px; border: 1px solid var(--border); border-radius: 6px;");
    htmlParts.push("      background: transparent; color: var(--muted); font-size: 12px; cursor: pointer;");
    htmlParts.push("      transition: all 0.15s;");
    htmlParts.push("    }");
    htmlParts.push("    .filter-btn:hover { border-color: var(--accent); color: var(--text); }");
    htmlParts.push("    .filter-btn.active { background: var(--accent); color: #0a0f0d; border-color: var(--accent); }");
    htmlParts.push("    .filter-btn-delta { border-style: dashed; }");
    htmlParts.push("    .filter-btn-delta[data-filter='new-failed'] { border-color: var(--fail); color: var(--fail); }");
    htmlParts.push("    .filter-btn-delta[data-filter='new-failed']:hover { background: rgba(239,68,68,0.15); }");
    htmlParts.push("    .filter-btn-delta[data-filter='new-failed'].active { background: var(--fail); color: white; border-style: solid; }");
    htmlParts.push("    .filter-btn-delta[data-filter='new-passed'] { border-color: var(--pass); color: var(--pass); }");
    htmlParts.push("    .filter-btn-delta[data-filter='new-passed']:hover { background: rgba(0,209,142,0.15); }");
    htmlParts.push("    .filter-btn-delta[data-filter='new-passed'].active { background: var(--pass); color: #0a0f0d; border-style: solid; }");
    htmlParts.push("    .test-items-list { overflow-y: auto; flex: 1; }");
    htmlParts.push("    .split-divider {");
    htmlParts.push("      width: 6px; background: var(--border); cursor: col-resize; flex-shrink: 0;");
    htmlParts.push("      border-radius: 3px; transition: background 0.2s;");
    htmlParts.push("    }");
    htmlParts.push("    .split-divider:hover { background: var(--accent); }");
    htmlParts.push("    .test-details-panel {");
    htmlParts.push("      flex: 1; min-width: 200px; overflow-y: auto; padding-left: 16px;");
    htmlParts.push("    }");
    htmlParts.push("    .no-test-selected {");
    htmlParts.push("      display: flex; align-items: center; justify-content: center; height: 100%;");
    htmlParts.push("      color: var(--muted); font-size: 14px; font-style: italic;");
    htmlParts.push("    }");
    htmlParts.push("    .details-header { margin-bottom: 16px; }");
    htmlParts.push("    .details-content { font-size: 14px; }");
    htmlParts.push("    .test-item {");
    htmlParts.push("      display: flex; align-items: center; gap: 10px; padding: 8px 12px; margin-bottom: 6px;");
    htmlParts.push("      background: rgba(255,255,255,0.03); border: 1px solid var(--border); border-radius: 8px;");
    htmlParts.push("      cursor: pointer; transition: all 0.15s;");
    htmlParts.push("    }");
    htmlParts.push("    .test-item:hover { background: rgba(255,255,255,0.06); }");
    htmlParts.push("    .test-item.selected { background: rgba(34,211,238,0.15); border-color: var(--accent); }");
    htmlParts.push("    .test-icon {");
    htmlParts.push("      width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0;");
    htmlParts.push("    }");
    htmlParts.push("    .test-item.status-passed .test-icon { background: var(--pass); }");
    htmlParts.push("    .test-item.status-failed .test-icon { background: var(--fail); }");
    htmlParts.push("    .test-item.status-skipped .test-icon { background: var(--skip); }");
    htmlParts.push("    .test-name { flex: 1; font-size: 13px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }");
    htmlParts.push("    .test-duration { font-size: 12px; color: var(--muted); }");
    htmlParts.push("    .delta-indicator {");
    htmlParts.push("      font-size: 11px; font-weight: 700; margin-left: 6px; padding: 2px 5px;");
    htmlParts.push("      border-radius: 4px; flex-shrink: 0;");
    htmlParts.push("    }");
    htmlParts.push("    .delta-improved { background: var(--pass); color: #0a0f0d; }");
    htmlParts.push("    .delta-regressed { background: var(--fail); color: white; }");
    htmlParts.push("    .delta-changed { background: var(--skip); color: #0a0f0d; }");
    htmlParts.push("    .test-modal {");
    htmlParts.push("      display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%;");
    htmlParts.push("      background: rgba(0,0,0,0.8); align-items: center; justify-content: center; padding: 20px;");
    htmlParts.push("    }");
    htmlParts.push("    .modal-dialog {");
    htmlParts.push("      background: var(--panel); border: 2px solid var(--border); border-radius: 16px;");
    htmlParts.push("      max-width: 900px; width: 100%; max-height: 90vh; overflow: hidden; display: flex; flex-direction: column;");
    htmlParts.push("      box-shadow: 0 20px 60px rgba(0,0,0,0.5);");
    htmlParts.push("    }");
    htmlParts.push("    .modal-header {");
    htmlParts.push("      display: flex; justify-content: space-between; align-items: start; padding: 20px; border-bottom: 2px solid var(--border);");
    htmlParts.push("    }");
    htmlParts.push("    .close-btn {");
    htmlParts.push("      background: none; border: none; color: var(--muted); font-size: 28px; cursor: pointer;");
    htmlParts.push("      line-height: 1; padding: 0; width: 32px; height: 32px; flex-shrink: 0;");
    htmlParts.push("    }");
    htmlParts.push("    .close-btn:hover { color: var(--text); }");
    htmlParts.push("    .modal-body { padding: 20px; overflow-y: auto; }");
    htmlParts.push("    .detail-row { display: flex; gap: 12px; margin-bottom: 12px; font-size: 14px; align-items: center; }");
    htmlParts.push("    .detail-label { font-weight: 600; color: var(--muted); min-width: 80px; }");
    htmlParts.push("    .detail-header-row { display: flex; align-items: flex-start; gap: 8px; margin-bottom: 12px; }");
    htmlParts.push("    .detail-label-row { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }");
    htmlParts.push("    .copy-btn {");
    htmlParts.push("      background: transparent; border: 1px solid var(--border); border-radius: 4px;");
    htmlParts.push("      padding: 4px 6px; cursor: pointer; color: var(--muted); transition: all 0.15s;");
    htmlParts.push("      display: flex; align-items: center; justify-content: center; flex-shrink: 0;");
    htmlParts.push("    }");
    htmlParts.push("    .copy-btn:hover { border-color: var(--accent); color: var(--accent); }");
    htmlParts.push("    .copy-btn.copied { border-color: var(--pass); color: var(--pass); }");
    htmlParts.push("    .detail-section { margin-top: 20px; }");
    htmlParts.push("    .detail-section .detail-label { margin-bottom: 8px; display: block; }");
    htmlParts.push("    .status-badge {");
    htmlParts.push("      padding: 4px 10px; border-radius: 6px; font-size: 12px; font-weight: 700; text-transform: uppercase;");
    htmlParts.push("    }");
    htmlParts.push("    .status-badge.status-passed { background: var(--pass); color: #0a0f0d; }");
    htmlParts.push("    .status-badge.status-failed { background: var(--fail); color: white; }");
    htmlParts.push("    .status-badge.status-skipped { background: var(--skip); color: #0a0f0d; }");
    htmlParts.push("    .error-text, .stack-trace, .output-text {");
    htmlParts.push("      background: #0a0f0d; padding: 12px; border-radius: 8px; overflow-x: auto;");
    htmlParts.push("      font-family: 'Consolas', 'Monaco', monospace; font-size: 12px; line-height: 1.5;");
    htmlParts.push("      white-space: pre-wrap; word-break: break-word;");
    htmlParts.push("    }");
    htmlParts.push("    .error-text { color: #ff6b6b; border-left: 3px solid var(--fail); }");
    htmlParts.push("    .stack-trace { color: #ffa500; border-left: 3px solid #ffa500; }");
    htmlParts.push("    .output-text { color: var(--muted); border-left: 3px solid var(--muted); }");
    htmlParts.push("    @media (max-width: 640px) {");
    htmlParts.push("      body { padding: 16px; }");
    htmlParts.push("      .children { grid-template-columns: 1fr; }");
    htmlParts.push("      .tests-container { flex-direction: column; height: auto; max-height: 600px; }");
    htmlParts.push("      .test-list-panel { width: 100%; max-height: 200px; padding-right: 0; padding-bottom: 8px; }");
    htmlParts.push("      .split-divider { width: 100%; height: 6px; cursor: row-resize; margin: 8px 0; }");
    htmlParts.push("      .test-details-panel { padding-left: 0; padding-top: 8px; }");
    htmlParts.push("    }");
    htmlParts.push("  </style>");
    htmlParts.push("</head>");
    htmlParts.push("<body>");
    // Build git info display string
    let gitInfoHtml = "";
    if (currentGitShort) {
        gitInfoHtml = ` <span style="font-size: 14px; color: var(--accent); font-weight: 500; margin-left: 12px;" title="Current commit">${currentGitShort}</span>`;
        if (prevGitShort && previous) {
            gitInfoHtml += `<span style="font-size: 14px; color: var(--muted); margin-left: 4px;">vs</span>`;
            gitInfoHtml += `<span style="font-size: 14px; color: #a78bfa; font-weight: 500; margin-left: 4px;" title="Comparison commit">${prevGitShort}</span>`;
        }
    }
    // Build run IDs display string (visible on hover)
    const copyIconSmall = '<svg viewBox="0 0 24 24" width="10" height="10"><path fill="currentColor" d="M16 1H4c-1.1 0-2 .9-2 2v14h2V3h12V1zm3 4H8c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h11c1.1 0 2-.9 2-2V7c0-1.1-.9-2-2-2zm0 16H8V7h11v14z"/></svg>';
    let runIdsHtml = "";
    if (currentRunId) {
        runIdsHtml = `<span class="run-ids">`;
        runIdsHtml += `<span class="run-id-item"><span class="run-id-value">${currentRunId}</span><button class="run-id-copy" onclick="navigator.clipboard.writeText('${currentRunId}').then(() => { this.classList.add('copied'); setTimeout(() => this.classList.remove('copied'), 1500); })" title="Copy run ID">${copyIconSmall}</button></span>`;
        if (prevRunId && previous) {
            runIdsHtml += `<span style="color: var(--muted);">vs</span>`;
            runIdsHtml += `<span class="run-id-item comparison"><span class="run-id-value">${prevRunId}</span><button class="run-id-copy" onclick="navigator.clipboard.writeText('${prevRunId}').then(() => { this.classList.add('copied'); setTimeout(() => this.classList.remove('copied'), 1500); })" title="Copy run ID">${copyIconSmall}</button></span>`;
        }
        runIdsHtml += `</span>`;
    }
    htmlParts.push("  <div class=\"header-container\"><h1>Ivy.EntityFrameworkCore.BigQuery tests <span style=\"font-size: 16px; color: var(--muted); font-weight: 400; margin-left: 12px;\">" + generatedAt + "</span>" + gitInfoHtml + "</h1>" + runIdsHtml + "<div class=\"search-container\"><input type=\"text\" id=\"error-search\" class=\"search-input\" placeholder=\"Search error messages...\" onkeyup=\"if(event.key==='Enter')searchErrors(this.value)\"><button class=\"search-clear\" onclick=\"clearSearch()\">Clear</button><span id=\"search-status\" class=\"search-status\"></span></div></div>");
    htmlParts.push("  <div class=\"legend\">");
    htmlParts.push("    <span class=\"dot\" style=\"background: var(--pass)\"></span> Passed");
    htmlParts.push("    <span class=\"dot\" style=\"background: var(--fail)\"></span> Failed");
    htmlParts.push("    <span class=\"dot\" style=\"background: var(--skip)\"></span> Skipped");
    if (previous) {
        htmlParts.push("    <span style=\"margin-left: 8px; opacity: 0.7\">│</span>");
        htmlParts.push("    <span class=\"dot\" style=\"background: #00a86b\"></span> Δ Improvement");
        htmlParts.push("    <span class=\"dot\" style=\"background: #ff6b6b\"></span> Δ Regression");
    }
    htmlParts.push("  </div>");
    htmlParts.push("  <div id=\"summary\" class=\"summary\"></div>");
    htmlParts.push("  <div id=\"groups\" class=\"groups\"></div>");
    htmlParts.push("  <script id=\"data\" type=\"application/json\">" + dataJson + "</script>");
    htmlParts.push("  <script>");
    htmlParts.push(clientScript);
    htmlParts.push("  </script>");
    htmlParts.push("</body>");
    htmlParts.push("</html>");

    return htmlParts.join("\n");
}

function loadCurrentGitInfo(historyPath) {
    if (!historyPath) return null;
    try {
        if (!fs.existsSync(historyPath)) return null;
        const data = JSON.parse(fs.readFileSync(historyPath, "utf8"));
        if (!Array.isArray(data) || data.length === 0) return null;
        const current = data[data.length - 1];
        return {
            commit: current.GitCommit || null,
            shortCommit: current.GitCommitShort || null
        };
    } catch {
        return null;
    }
}

function main() {
    const args = parseArgs(process.argv.slice(2));
    let files = [...args.files];
    if (args.dir) {
        files = files.concat(findTrxFiles(args.dir));
    }
    if (files.length === 0) {
        console.error("No TRX files provided. Use --merged <file> or --dir <folder>.");
        process.exit(1);
    }

    const model = buildModel(files);
    const prev = loadHistory(args.history, args.compareRun);
    const prevTests = loadPreviousTests(args.history, args.compareRun);

    // Collect all tests from the model
    const allTests = model.parents.flatMap(p => p.tests);

    // Update history with current test results
    updateHistoryWithTests(args.history, allTests);

    // Load git commit info for current and comparison runs
    const currentGitInfo = loadCurrentGitInfo(args.history);
    const prevGitInfo = prev ? {
        commit: prev.GitCommit || null,
        shortCommit: prev.GitCommitShort || null
    } : null;

    // Compute run IDs
    const currentRunId = model.testRunDate ? timestampToRunId(model.testRunDate.toISOString()) : null;
    const prevRunId = prev?.Timestamp ? timestampToRunId(prev.Timestamp) : null;

    const html = renderHtml(model, prev, prevTests, currentGitInfo, prevGitInfo, currentRunId, prevRunId);
    fs.writeFileSync(args.out, html, "utf8");
    console.log(`Dashboard written to ${args.out} (files: ${files.length})`);
}

if (require.main === module) {
    main();
}
