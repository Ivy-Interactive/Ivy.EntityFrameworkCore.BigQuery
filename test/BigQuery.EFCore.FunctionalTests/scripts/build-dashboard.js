#!/usr/bin/env node
// Generates an HTML dashboard from one or more TRX files.
// Usage:
//   node build-dashboard.js --merged TestResults\\TestResults.trx --history TestResults\\history.json --out TestResults\\dashboard.html
//   node build-dashboard.js --dir TestResults --history TestResults\\history.json --out TestResults\\dashboard.html

const fs = require("fs");
const path = require("path");
const { XMLParser } = require("fast-xml-parser");

function parseArgs(argv) {
    const args = { files: [], dir: null, out: "dashboard.html", history: null, hideChildren: false };
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

function loadHistory(historyPath) {
    if (!historyPath) return null;
    try {
        if (!fs.existsSync(historyPath)) return null;
        const data = JSON.parse(fs.readFileSync(historyPath, "utf8"));
        if (!Array.isArray(data) || data.length < 2) return null;
        return data[data.length - 2];
    } catch {
        return null;
    }
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
        const name = method.name || method.Name || test.name || test.Name || "Unnamed";
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

        records.push({
            className: meta.className,
            testName: meta.name,
            status,
            durationSec,
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
            });
        }
        const child = parent.children.get(childName);

        child[rec.status.toLowerCase()] += 1;
        child.total += 1;
        child.durationSec += rec.durationSec;

        parent[rec.status.toLowerCase()] += 1;
        parent.total += 1;
        parent.durationSec += rec.durationSec;
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

    return { parents, totals, files };
}

function renderHtml(model, previous = null) {
    const dataJson = JSON.stringify(model);
    const prevJson = previous ? JSON.stringify(previous) : "null";

    const clientScript = [
        "const model = JSON.parse(document.getElementById('data').textContent);",
        "const previous = " + prevJson + ";",
        "const formatPct = (part, total) => total ? Math.round((part / total) * 100) : 0;",
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
        "  totalBarHtml += '<span>Δ Passed ' + deltaTotalPass + ', Failed ' + deltaTotalFail + '</span>';",
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
        "  const deltaLabel = prevParent ? ('Δ Passed ' + (parent.passed - prevPPass) + ', Failed ' + (parent.failed - prevPFail)) : '';",
        "  const childHtml = parent.children.map(child => {",
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
        "    let left = 0;",
        "    let html = '';",
        "    html += '<div class=\"child\">';",
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
        "      html += '<span>Δ ' + (child.passed - prevPass) + '/' + (child.failed - prevFail) + '</span>';",
        "    }",
        "    html += '</div></div>';",
        "    return html;",
        "  }).join('');",
        "  let card = '';",
        "  card += '<div class=\"card\">';",
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
        "  if (!hideChildren) { card += '<div class=\"children\">' + childHtml + '</div>'; }",
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
    htmlParts.push("  <title> EF Core Test Dashboard</title>");
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
    htmlParts.push("    .summary {");
    htmlParts.push("      display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));");
    htmlParts.push("      gap: 12px; margin-bottom: 16px;");
    htmlParts.push("    }");
    htmlParts.push("    .badge { padding: 12px 14px; background: var(--panel); border: 2px solid var(--border); border-radius: 12px; box-shadow: var(--shadow); }");
    htmlParts.push("    .badge .label { color: var(--muted); font-size: 13px; }");
    htmlParts.push("    .badge .value { font-size: 20px; font-weight: 700; margin-top: 4px; }");
    htmlParts.push("    .groups { display: flex; flex-direction: column; gap: 14px; }");
    htmlParts.push("    .card {");
    htmlParts.push("      background: var(--panel);");
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
    htmlParts.push("    .meta { color: var(--muted); font-size: 13px; display: flex; gap: 10px; flex-wrap: wrap; }");
    htmlParts.push("    .children { margin-top: 10px; display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 10px; }");
    htmlParts.push("    .child { padding: 10px 12px; background: rgba(255,255,255,0.02); border: 1px solid var(--border); border-radius: 12px; }");
    htmlParts.push("    .child .title { font-weight: 600; font-size: 14px; margin-bottom: 6px; color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }");
    htmlParts.push("    .legend { display: flex; gap: 12px; align-items: center; margin: 10px 0 18px; color: var(--muted); font-size: 13px; }");
    htmlParts.push("    .dot { width: 12px; height: 12px; border-radius: 999px; display: inline-block; }");
    htmlParts.push("    @media (max-width: 640px) { body { padding: 16px; } .children { grid-template-columns: 1fr; } }");
    htmlParts.push("  </style>");
    htmlParts.push("</head>");
    htmlParts.push("<body>");
    htmlParts.push("  <h1>Ivy.EntityFrameworkCore.BigQuery tests</h1>");
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
    const prev = loadHistory(args.history);
    const html = renderHtml(model, prev);
    fs.writeFileSync(args.out, html, "utf8");
    console.log(`Dashboard written to ${args.out} (files: ${files.length})`);
}

if (require.main === module) {
    main();
}
