#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

const usage = `Usage:
  node observable.mjs fetch <viewer-or-endpoint-or-id> --out <file.json>
  node observable.mjs probe <file.json>
  node observable.mjs summarize <file.json>
  node observable.mjs compare <file-a.json> <file-b.json>
  node observable.mjs align|joint <spark-probe-or-analysis.json> <observable.json>`;

function fail(message) {
  console.error(`Error: ${message}`);
  console.error(usage);
  process.exitCode = 1;
}

function readJson(file) {
  const raw = fs.readFileSync(file);
  let value;
  try {
    value = JSON.parse(raw.toString('utf8'));
  } catch (error) {
    throw new Error(`invalid JSON in ${file}: ${error.message}`);
  }
  validatePayload(value, file);
  return { value, raw };
}

function readAnyJson(file) {
  const raw = fs.readFileSync(file);
  try {
    return JSON.parse(raw.toString('utf8'));
  } catch (error) {
    throw new Error(`invalid JSON in ${file}: ${error.message}`);
  }
}

function validatePayload(value, label = 'payload') {
  if (!value || typeof value !== 'object' || !value.data || !value.diagnostics) {
    throw new Error(`${label} is not an Observable /v1/get payload (expected data and diagnostics)`);
  }
  for (const key of ['entities', 'blocks', 'traces']) {
    if (!(key in value.data)) throw new Error(`${label} is missing data.${key}`);
  }
  for (const key of ['entities', 'blocks']) {
    if (typeof value.data[key] !== 'object' || Array.isArray(value.data[key]) || value.data[key] === null) {
      throw new Error(`${label} data.${key} must be an object of group arrays`);
    }
  }
}

const TOP_LEVEL_KEYS = new Set(['data', 'diagnostics']);
const DATA_KEYS = new Set(['entities', 'blocks', 'traces', 'ticks']);
const DIAGNOSTIC_KEYS = new Set([
  'user',
  'start',
  'duration',
  'minecraftVersion',
  'modLoader',
  'observableVersion',
  'additionalDiagnostics',
]);
const ADDITIONAL_DIAGNOSTIC_KEYS = new Set(['System Report', 'Mods']);
const ROW_KEYS = new Set(['position', 'type', 'rate', 'ticks', 'traces', 'entityId']);
const POSITION_KEYS = new Set(['x', 'y', 'z']);
const TRACE_KEYS = new Set(['className', 'methodName', 'children', 'count']);

function pushUnique(list, value, limit = 100) {
  if (list.length < limit && !list.includes(value)) list.push(value);
}

function schemaAudit(value) {
  const audit = {
    unknownTopLevel: [],
    unknownData: [],
    unknownDiagnostics: [],
    unknownAdditionalDiagnostics: [],
    unknownRowFields: [],
    unknownPositionFields: [],
    unknownTraceFields: [],
    invalidTypes: [],
    invalidPositions: 0,
    invalidTraceNodes: 0,
    invalidTraceChildren: 0,
    traceNodesInspected: 0,
    traceInspectionLimited: false,
  };
  if (!value || typeof value !== 'object') return audit;
  for (const key of Object.keys(value)) {
    if (!TOP_LEVEL_KEYS.has(key)) pushUnique(audit.unknownTopLevel, key);
  }
  const data = value.data;
  if (!data || typeof data !== 'object' || Array.isArray(data)) {
    pushUnique(audit.invalidTypes, 'data');
    return audit;
  }
  for (const key of Object.keys(data)) {
    if (!DATA_KEYS.has(key)) pushUnique(audit.unknownData, key);
  }
  const diagnostics = value.diagnostics;
  if (!diagnostics || typeof diagnostics !== 'object' || Array.isArray(diagnostics)) {
    pushUnique(audit.invalidTypes, 'diagnostics');
  } else {
    for (const key of Object.keys(diagnostics)) {
      if (!DIAGNOSTIC_KEYS.has(key)) pushUnique(audit.unknownDiagnostics, key);
    }
    const additional = diagnostics.additionalDiagnostics;
    if (additional !== undefined) {
      if (!additional || typeof additional !== 'object' || Array.isArray(additional)) {
        pushUnique(audit.invalidTypes, 'diagnostics.additionalDiagnostics');
      } else {
        for (const key of Object.keys(additional)) {
          if (!ADDITIONAL_DIAGNOSTIC_KEYS.has(key)) pushUnique(audit.unknownAdditionalDiagnostics, key);
        }
      }
    }
    for (const key of ['start', 'duration']) {
      if (diagnostics[key] !== undefined) {
        const n = Number(diagnostics[key]);
        if (!['number', 'string'].includes(typeof diagnostics[key]) ||
            !Number.isFinite(n) || n < 0) {
          pushUnique(audit.invalidTypes, `diagnostics.${key}`);
        }
      }
    }
  }
  if (data.ticks !== undefined) {
    const n = Number(data.ticks);
    if (!['number', 'string'].includes(typeof data.ticks) ||
        !Number.isFinite(n) || n <= 0) {
      pushUnique(audit.invalidTypes, 'data.ticks');
    }
  }
  const traceBudget = { remaining: 20000 };
  const inspectTrace = (node) => {
    if (traceBudget.remaining <= 0) {
      audit.traceInspectionLimited = true;
      return;
    }
    traceBudget.remaining -= 1;
    audit.traceNodesInspected += 1;
    if (!node || typeof node !== 'object' || Array.isArray(node)) {
      audit.invalidTraceNodes += 1;
      return;
    }
    for (const key of Object.keys(node)) {
      if (!TRACE_KEYS.has(key)) pushUnique(audit.unknownTraceFields, key);
    }
    if (node.className !== undefined && typeof node.className !== 'string') {
      pushUnique(audit.invalidTypes, 'trace.className');
    }
    if (node.methodName !== undefined && typeof node.methodName !== 'string') {
      pushUnique(audit.invalidTypes, 'trace.methodName');
    }
    if (node.count !== undefined) {
      const count = Number(node.count);
      if (!Number.isFinite(count) || count < 0) pushUnique(audit.invalidTypes, 'trace.count');
    }
    if (node.children !== undefined && !Array.isArray(node.children)) {
      audit.invalidTraceChildren += 1;
      return;
    }
    for (const child of node.children || []) inspectTrace(child);
  };
  inspectTrace(data.traces);
  for (const key of ['blocks', 'entities']) {
    const groups = data[key];
    if (!groups || typeof groups !== 'object' || Array.isArray(groups)) continue;
    for (const [group, rows] of Object.entries(groups)) {
      if (!Array.isArray(rows)) continue;
      for (const row of rows) {
        if (!row || typeof row !== 'object' || Array.isArray(row)) continue;
        for (const rowKey of Object.keys(row)) {
          if (!ROW_KEYS.has(rowKey)) pushUnique(audit.unknownRowFields, rowKey);
        }
        if (row.position !== undefined) {
          if (!row.position || typeof row.position !== 'object' || Array.isArray(row.position)) {
            audit.invalidPositions += 1;
          } else {
            for (const positionKey of Object.keys(row.position)) {
              if (!POSITION_KEYS.has(positionKey)) pushUnique(audit.unknownPositionFields, positionKey);
            }
            for (const positionKey of POSITION_KEYS) {
              const n = Number(row.position[positionKey]);
              if (!Number.isFinite(n)) audit.invalidPositions += 1;
            }
          }
        }
        if (row.type !== undefined && typeof row.type !== 'string') {
          pushUnique(audit.invalidTypes, `${key}.type`);
        }
        for (const numericKey of ['rate', 'ticks']) {
          if (row[numericKey] !== undefined) {
            const numericValue = Number(row[numericKey]);
            if (!['number', 'string'].includes(typeof row[numericKey]) ||
                !Number.isFinite(numericValue)) {
              pushUnique(audit.invalidTypes, `${key}.${numericKey}`);
            }
          }
        }
        if (row.entityId !== undefined &&
            !['string', 'number'].includes(typeof row.entityId)) {
          pushUnique(audit.invalidTypes, `${key}.entityId`);
        }
        if (row.traces !== undefined) inspectTrace(row.traces);
      }
    }
  }
  return audit;
}

function normalizedText(value, sortLines = false) {
  if (typeof value !== 'string') return '';
  const lines = value.replace(/\r\n?/g, '\n').split('\n').map(line => line.trim()).filter(Boolean);
  if (sortLines) lines.sort();
  return lines.join('\n');
}

function textFingerprint(value, sortLines = false) {
  const normalized = normalizedText(value, sortLines);
  return normalized
    ? { present: true, sha256: sha256(Buffer.from(normalized, 'utf8')), lines: normalized.split('\n').length }
    : { present: false, sha256: null, lines: 0 };
}

function systemEnvironmentFingerprint(text) {
  const normalized = normalizedText(text);
  const selected = normalized.split('\n').filter(line =>
    /^(Minecraft Version|Operating System|Java Version|Java VM Version|CPUs|Processor Vendor|Processor Name|JVM Flags):/.test(line));
  return textFingerprint(selected.join('\n'));
}

function newIssueTracker() {
  return {
    invalidGroups: 0,
    invalidRows: 0,
    invalidRate: 0,
    invalidTicks: 0,
    missingRate: 0,
    missingTicks: 0,
    duplicateKeys: 0,
    overrangeEntries: 0,
    overrangeExtraTicks: 0,
    numericStringRate: 0,
    numericStringTicks: 0,
    fractionalTicks: 0,
    fractionalTotalTicks: 0,
  };
}

function schemaFingerprint(value) {
  const data = value?.data ?? {};
  const diagnostics = value?.diagnostics ?? {};
  const rowKeys = new Set();
  const positionKeys = new Set();
  for (const kind of ['blocks', 'entities']) {
    for (const rows of Object.values(data[kind] ?? {})) {
      if (!Array.isArray(rows)) continue;
      for (const row of rows.slice(0, 200)) {
        if (!row || typeof row !== 'object') continue;
        Object.keys(row).forEach(key => rowKeys.add(key));
        if (row.position && typeof row.position === 'object') {
          Object.keys(row.position).forEach(key => positionKeys.add(key));
        }
      }
    }
  }
  const traceKeys = new Set();
  const visit = (node, budget = { remaining: 1000 }) => {
    if (!node || typeof node !== 'object' || Array.isArray(node) || budget.remaining-- <= 0) return;
    Object.keys(node).forEach(key => traceKeys.add(key));
    for (const child of node.children || []) visit(child, budget);
  };
  visit(data.traces);
  const shape = {
    top: Object.keys(value ?? {}).sort(),
    data: Object.keys(data).sort(),
    diagnostics: Object.keys(diagnostics).sort(),
    row: [...rowKeys].sort(),
    position: [...positionKeys].sort(),
    trace: [...traceKeys].sort(),
  };
  return { sha256: sha256(Buffer.from(JSON.stringify(shape), 'utf8')), shape };
}

function reportFingerprints(value) {
  const additional = value?.diagnostics?.additionalDiagnostics ?? {};
  const mods = textFingerprint(additional.Mods, true);
  const system = textFingerprint(additional['System Report']);
  const environment = systemEnvironmentFingerprint(additional['System Report']);
  return {
    mods,
    system,
    environment,
    schema: schemaFingerprint(value),
  };
}

function idFromInput(input) {
  const text = String(input).trim();
  const endpoint = text.match(/observable\.tas\.sh\/v1\/get\/([^/?#]+)/i);
  if (endpoint) return endpoint[1];
  const viewer = text.match(/observable\.tas\.sh\/p\/([^/?#]+)/i);
  if (viewer) return viewer[1];
  if (/^[A-Za-z0-9_-]+$/.test(text)) return text;
  return null;
}

function endpointFromInput(input) {
  const text = String(input).trim();
  if (/\/v1\/get\//i.test(text)) {
    const url = new URL(text);
    if (url.hostname !== 'observable.tas.sh' || !/^\/v1\/get\/[^/]+$/i.test(url.pathname)) {
      throw new Error('only observable.tas.sh/v1/get/<id> endpoints are accepted');
    }
    return url.toString();
  }
  const id = idFromInput(text);
  if (id) return `https://observable.tas.sh/v1/get/${id}`;
  throw new Error(`cannot derive Observable report id from ${input}`);
}

function sha256(buffer) {
  return crypto.createHash('sha256').update(buffer).digest('hex');
}

function allRows(data, key, issues) {
  const result = [];
  for (const [group, rows] of Object.entries(data[key] ?? {})) {
    if (!Array.isArray(rows)) {
      issues.invalidGroups += 1;
      continue;
    }
    const seen = new Set();
    for (const row of rows) {
      if (!row || typeof row !== 'object' || Array.isArray(row)) {
        issues.invalidRows += 1;
        continue;
      }
      const rawRate = row.rate;
      const rawTicks = row.ticks;
      const rate = Number(rawRate);
      const ticks = Number(rawTicks);
      if (typeof rawRate === 'string' && rawRate.trim() !== '' && Number.isFinite(rate)) {
        issues.numericStringRate += 1;
      }
      if (typeof rawTicks === 'string' && rawTicks.trim() !== '' && Number.isFinite(ticks)) {
        issues.numericStringTicks += 1;
      }
      if (Number.isFinite(ticks) && !Number.isInteger(ticks)) issues.fractionalTicks += 1;
      if (!('rate' in row)) issues.missingRate += 1;
      if (!('ticks' in row)) issues.missingTicks += 1;
      if (rawRate === null || rawRate === '' || !Number.isFinite(rate) || rate < 0) issues.invalidRate += 1;
      if (rawTicks === null || rawTicks === '' || !Number.isFinite(ticks) || ticks < 0) issues.invalidTicks += 1;
      const identity = row.entityId !== undefined
        ? `${group}|entity|${row.entityId}`
        : row.position
          ? `${group}|block|${row.position.x},${row.position.y},${row.position.z}|${row.type ?? ''}`
          : null;
      if (identity && seen.has(identity)) issues.duplicateKeys += 1;
      if (identity) seen.add(identity);
      result.push({
        group,
        ...row,
        _rate: rawRate !== null && rawRate !== '' && Number.isFinite(rate) && rate >= 0 ? rate : null,
        _ticks: rawTicks !== null && rawTicks !== '' && Number.isFinite(ticks) && ticks >= 0 ? ticks : null,
      });
    }
  }
  return result;
}

function maxTicks(rows) {
  return rows.reduce((max, row) => {
    return row._ticks === null ? max : Math.max(max, row._ticks);
  }, 0);
}

function resolveTicks(value, rows, issues = null) {
  const rawExplicitTicks = value?.data?.ticks;
  const explicitTicks = Number(rawExplicitTicks);
  if (issues && typeof rawExplicitTicks === 'string' && rawExplicitTicks.trim() !== '' &&
      Number.isFinite(explicitTicks)) {
    issues.numericStringTicks += 1;
  }
  if (issues && Number.isFinite(explicitTicks) && !Number.isInteger(explicitTicks)) {
    issues.fractionalTotalTicks += 1;
  }
  if (Number.isFinite(explicitTicks) && explicitTicks > 0) {
    return {
      ticks: explicitTicks,
      source: 'data.ticks (observed)',
    };
  }
  const inferredTicks = maxTicks(rows);
  return {
    ticks: inferredTicks,
    source: inferredTicks ? 'max(entry.ticks) (inferred)' : 'missing',
  };
}

function markOverrange(rows, totalTicks, issues) {
  if (!totalTicks) return;
  for (const row of rows) {
    row._overrange = row._ticks !== null && row._ticks > totalTicks;
    row._overrangeRatio = row._overrange ? row._ticks / totalTicks : null;
    if (!row._overrange) continue;
    issues.overrangeEntries += 1;
    issues.overrangeExtraTicks += row._ticks - totalTicks;
  }
}

function overrangeAudit(rows, totalTicks) {
  const excluded = rows.filter(row => row._overrange && row._rate !== null && row._ticks !== null);
  const byGroup = new Map();
  const byType = new Map();
  const add = (map, key, row, fullNs, clampedNs) => {
    const stats = map.get(key) ?? { entries: 0, extraTicks: 0, fullNs: 0, clampedNs: 0, maxRatio: 0 };
    stats.entries += 1;
    stats.extraTicks += row._ticks - totalTicks;
    stats.fullNs += fullNs;
    stats.clampedNs += clampedNs;
    stats.maxRatio = Math.max(stats.maxRatio, row._overrangeRatio);
    map.set(key, stats);
  };
  let fullNs = 0;
  let clampedNs = 0;
  let extraTicks = 0;
  let minRatio = Infinity;
  let maxRatio = 0;
  for (const row of excluded) {
    const full = row._rate * row._ticks / totalTicks;
    const clamped = row._rate;
    fullNs += full;
    clampedNs += clamped;
    extraTicks += row._ticks - totalTicks;
    minRatio = Math.min(minRatio, row._overrangeRatio);
    maxRatio = Math.max(maxRatio, row._overrangeRatio);
    add(byGroup, row.group ?? '(unknown)', row, full, clamped);
    add(byType, row.type ?? '(unknown)', row, full, clamped);
  }
  const render = map => [...map.entries()]
    .sort((a, b) => b[1].fullNs - a[1].fullNs)
    .map(([name, stats]) => ({
      name,
      entries: stats.entries,
      extraTicks: stats.extraTicks,
      fullReportedMsPerTick: stats.fullNs / 1e6,
      clampedMsPerTick: stats.clampedNs / 1e6,
      maxRatio: stats.maxRatio,
    }));
  return {
    entries: excluded.length,
    extraTicks,
    fullReportedMsPerTick: fullNs / 1e6,
    clampedMsPerTick: clampedNs / 1e6,
    minRatio: Number.isFinite(minRatio) ? minRatio : null,
    maxRatio: maxRatio || null,
    byGroup: render(byGroup),
    byType: render(byType),
  };
}

function printOverrange(label, rows, totalTicks) {
  const excluded = rows.filter(row => row._overrange);
  if (!excluded.length) return;
  const audit = overrangeAudit(rows, totalTicks);
  console.log(`overrange audit (${label}): ${audit.entries} entries where ticks > ${totalTicks}; ` +
    `ratio=${fmt(audit.minRatio, 3)}..${fmt(audit.maxRatio, 3)}; extraTicks=${audit.extraTicks}`);
  console.log(`  conservative excluded impact: full-reported=${fmt(audit.fullReportedMsPerTick)} ms/tick, ` +
    `clamped-to-window=${fmt(audit.clampedMsPerTick)} ms/tick; neither is included in totals`);
  console.log('  by group:');
  for (const row of audit.byGroup.slice(0, 20)) {
    console.log(`    ${row.name}: entries=${row.entries} ratio<=${fmt(row.maxRatio, 3)} ` +
      `full=${fmt(row.fullReportedMsPerTick)} ms/tick clamped=${fmt(row.clampedMsPerTick)} ms/tick`);
  }
  console.log('  by type:');
  for (const row of audit.byType.slice(0, 20)) {
    console.log(`    ${row.name}: entries=${row.entries} ratio<=${fmt(row.maxRatio, 3)} ` +
      `full=${fmt(row.fullReportedMsPerTick)} ms/tick clamped=${fmt(row.clampedMsPerTick)} ms/tick`);
  }
  console.log(`  examples (maximum 50):`);
  for (const row of excluded.slice(0, 50)) {
    const pos = row.position ? ` @${row.position.x},${row.position.y},${row.position.z}` : '';
    console.log(`    excluded ${row.group} ${row.type || '(unknown)'} ticks=${row.ticks ?? 'N/A'} ` +
      `ratio=${fmt(row._overrangeRatio, 3)} rate=${row._rate === null ? 'N/A' : row._rate}${pos}`);
  }
  if (excluded.length > 50) {
    console.log(`    ... ${excluded.length - 50} more overrange entries omitted`);
  }
}

function weightedRate(rows, totalTicks) {
  if (!totalTicks) return 0;
  return rows.reduce((sum, row) => {
    if (row._rate === null || row._ticks === null || row._ticks > totalTicks) return sum;
    return sum + row._rate * row._ticks / totalTicks;
  }, 0);
}

function grouped(rows, key, totalTicks) {
  const sums = new Map();
  for (const row of rows) {
    const value = row[key] ?? '(unknown)';
    const rate = row._rate;
    const ticks = row._ticks;
    if (rate === null || ticks === null || ticks > totalTicks) continue;
    const entry = sums.get(value) ?? { activeTicks: 0, activeNs: 0, weightedNs: 0, entries: 0, maxEntryNs: 0 };
    entry.entries += 1;
    entry.activeTicks += ticks;
    entry.activeNs += rate * ticks;
    entry.weightedNs += rate * ticks / (totalTicks || 1);
    entry.maxEntryNs = Math.max(entry.maxEntryNs, rate);
    sums.set(value, entry);
  }
  return [...sums.entries()]
    .sort((a, b) => b[1].weightedNs - a[1].weightedNs)
    .map(([name, stats]) => ({
      name,
      entries: stats.entries,
      activeTicks: stats.activeTicks,
      activeNs: stats.activeNs,
      msPerTick: stats.weightedNs / 1e6,
      maxEntryMs: stats.maxEntryNs / 1e6,
    }));
}

function fmt(value, digits = 3) {
  return Number.isFinite(value) ? value.toFixed(digits) : 'N/A';
}

function rowKey(row) {
  if (row.entityId !== undefined) return `${row.group}|entity|${row.entityId}`;
  if (row.position) return `${row.group}|block|${row.position.x},${row.position.y},${row.position.z}|${row.type ?? ''}`;
  return `${row.group}|type|${row.type ?? '(unknown)'}`;
}

function namespaceCandidate(row) {
  const type = typeof row.type === 'string' ? row.type : '';
  const separator = type.indexOf(':');
  return separator > 0 ? type.slice(0, separator) : '(unknown)';
}

function perEntryTraceStats(rows) {
  const stats = {
    entries: rows.length,
    traceObjects: 0,
    countNonzero: 0,
    countZero: 0,
    countInvalid: 0,
    childless: 0,
    pathNodes: 0,
    pathLimited: false,
    pathCounts: new Map(),
  };
  const pathBudget = { remaining: 100000 };
  for (const row of rows) {
    const trace = row.traces;
    if (!trace || typeof trace !== 'object' || Array.isArray(trace)) {
      stats.countInvalid += 1;
      continue;
    }
    stats.traceObjects += 1;
    const count = Number(trace.count);
    if (!Number.isFinite(count) || count < 0) stats.countInvalid += 1;
    else if (count > 0) stats.countNonzero += 1;
    else stats.countZero += 1;
    if (!Array.isArray(trace.children) || trace.children.length === 0) stats.childless += 1;
    const seenPaths = new Set();
    const visit = (node, path = [], depth = 0) => {
      if (!node || typeof node !== 'object' || Array.isArray(node)) return;
      if (pathBudget.remaining <= 0) {
        stats.pathLimited = true;
        return;
      }
      if (depth > 64) {
        stats.pathLimited = true;
        return;
      }
      pathBudget.remaining -= 1;
      stats.pathNodes += 1;
      const key = [node.className, node.methodName].filter(Boolean).join('.');
      const nextPath = key ? [...path, key] : path;
      const pathKey = nextPath.join(' -> ');
      const nodeCount = Number(node.count);
      if (pathKey && Number.isFinite(nodeCount) && nodeCount >= 0 && !seenPaths.has(pathKey)) {
        seenPaths.add(pathKey);
        const entry = stats.pathCounts.get(pathKey) ?? { maxCount: 0, entries: 0 };
        entry.maxCount = Math.max(entry.maxCount, nodeCount);
        entry.entries += 1;
        stats.pathCounts.set(pathKey, entry);
      }
      if (!Array.isArray(node.children)) return;
      for (const child of node.children) visit(child, nextPath, depth + 1);
    };
    visit(trace);
  }
  stats.coverageRatio = stats.entries ? stats.traceObjects / stats.entries : null;
  stats.nonzeroRatio = stats.entries ? stats.countNonzero / stats.entries : null;
  return stats;
}

function printPerEntryTraceStats(label, rows, pathLimit = 20) {
  const stats = perEntryTraceStats(rows);
  console.log(`per-entry trace coverage (${label}): entries=${stats.entries} ` +
    `traceObjects=${stats.traceObjects} coverage=${fmt((stats.coverageRatio ?? NaN) * 100, 2)}% ` +
    `count>0=${stats.countNonzero} count=0=${stats.countZero} invalid=${stats.countInvalid} ` +
    `childless=${stats.childless} pathNodes=${stats.pathNodes} pathLimited=${stats.pathLimited}`);
  if (stats.pathCounts.size) {
    console.log(`per-entry trace exact paths (${label}; max node count, paths are not additive):`);
    for (const [key, path] of [...stats.pathCounts.entries()]
      .sort((a, b) => b[1].maxCount - a[1].maxCount || b[1].entries - a[1].entries)
      .slice(0, pathLimit)) {
      console.log(`  max=${String(path.maxCount).padStart(6)} entries=${String(path.entries).padStart(5)} ${key}`);
    }
  }
}

function entityChurn(aRows, bRows) {
  const aEntities = aRows.filter(row => row.entityId !== undefined);
  const bEntities = bRows.filter(row => row.entityId !== undefined);
  const aIds = new Set(aEntities.map(row => `${row.group}|${row.entityId}`));
  const bIds = new Set(bEntities.map(row => `${row.group}|${row.entityId}`));
  const stable = [...aIds].filter(id => bIds.has(id));
  const added = [...bIds].filter(id => !aIds.has(id));
  const removed = [...aIds].filter(id => !bIds.has(id));
  const denominator = Math.max(aIds.size, bIds.size);
  const aggregate = rows => {
    const map = new Map();
    for (const row of rows) {
      const key = `${row.group}|${row.type ?? '(unknown)'}`;
      const entry = map.get(key) ?? { group: row.group, type: row.type ?? '(unknown)', entries: 0, ids: new Set() };
      entry.entries += 1;
      if (row.entityId !== undefined) entry.ids.add(String(row.entityId));
      map.set(key, entry);
    }
    return map;
  };
  const ag = aggregate(aEntities);
  const bg = aggregate(bEntities);
  const aggregateDiffs = [];
  for (const key of new Set([...ag.keys(), ...bg.keys()])) {
    const a = ag.get(key);
    const b = bg.get(key);
    const aCount = a?.entries ?? 0;
    const bCount = b?.entries ?? 0;
    if (aCount !== bCount) {
      aggregateDiffs.push({
        key,
        group: b?.group ?? a?.group,
        type: b?.type ?? a?.type,
        aEntries: aCount,
        bEntries: bCount,
        delta: bCount - aCount,
      });
    }
  }
  aggregateDiffs.sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta));
  return {
    aEntries: aEntities.length,
    bEntries: bEntities.length,
    aUniqueIds: aIds.size,
    bUniqueIds: bIds.size,
    stableIds: stable.length,
    addedIds: added.length,
    removedIds: removed.length,
    stableRatio: denominator ? stable.length / denominator : null,
    aggregateDiffs,
    idChurnWarning: (aIds.size || bIds.size) > 0 && stable.length / Math.max(aIds.size, bIds.size, 1) < 0.5,
  };
}

function printEntityChurn(label, aRows, bRows) {
  const churn = entityChurn(aRows, bRows);
  console.log(`${label} entity ID churn: A=${churn.aUniqueIds} B=${churn.bUniqueIds} ` +
    `stable=${churn.stableIds} added=${churn.addedIds} removed=${churn.removedIds} ` +
    `stableRatio=${fmt((churn.stableRatio ?? NaN) * 100, 2)}%`);
  if (churn.idChurnWarning) {
    console.log(`  warning: exact entity IDs are unstable; use group/type aggregates for persistence, ` +
      `not added/removed IDs as spawn/despawn evidence`);
  }
  if (churn.aggregateDiffs.length) {
    console.log(`  entity group/type count deltas (B - A):`);
    for (const row of churn.aggregateDiffs.slice(0, 30)) {
      console.log(`    ${row.delta >= 0 ? '+' : ''}${row.delta} ${row.group} ${row.type} ` +
        `(A=${row.aEntries} B=${row.bEntries})`);
    }
  }
}

function globalTraceStats(trace) {
  const counts = new Map();
  const pathCounts = new Map();
  const limits = {
    visited: 0,
    maxDepth: 0,
    depthLimited: 0,
    nodeLimited: 0,
    malformedNodes: 0,
    malformedChildren: 0,
    rootInvalid: 0,
  };
  const visit = (node, path = [], depth = 0) => {
    if (!node || typeof node !== 'object' || Array.isArray(node)) {
      limits.malformedNodes += 1;
      return;
    }
    if (limits.visited >= 100000) {
      limits.nodeLimited += 1;
      return;
    }
    if (depth > 64) {
      limits.depthLimited += 1;
      return;
    }
    limits.visited += 1;
    limits.maxDepth = Math.max(limits.maxDepth, depth);
    const key = [node.className, node.methodName].filter(Boolean).join('.');
    const nextPath = key ? [...path, key] : path;
    const count = Number(node.count);
    if (key && Number.isFinite(count) && count >= 0) {
      counts.set(key, Math.max(counts.get(key) || 0, count));
      pathCounts.set(nextPath.join(' -> '), Math.max(pathCounts.get(nextPath.join(' -> ')) || 0, count));
    }
    if (node.children !== undefined && !Array.isArray(node.children)) {
      limits.malformedChildren += 1;
      return;
    }
    for (const child of node.children || []) visit(child, nextPath, depth + 1);
  };
  if (!trace || typeof trace !== 'object' || Array.isArray(trace)) limits.rootInvalid += 1;
  else visit(trace);
  return {
    counts,
    pathCounts,
    limits,
    truncated: limits.depthLimited > 0 || limits.nodeLimited > 0 || limits.malformedChildren > 0,
  };
}

function summary(value) {
  const diagnostics = value.diagnostics ?? {};
  const issues = newIssueTracker();
  const schema = schemaAudit(value);
  const blocks = allRows(value.data, 'blocks', issues);
  const entities = allRows(value.data, 'entities', issues);
  const tickInfo = resolveTicks(value, [...blocks, ...entities], issues);
  const analysisTicks = tickInfo.ticks;
  markOverrange(blocks, analysisTicks, issues);
  markOverrange(entities, analysisTicks, issues);
  const durationMs = Number(diagnostics.duration);
  const tickSource = tickInfo.source;
  const effectiveTps = Number.isFinite(durationMs) && analysisTicks
    ? analysisTicks / (durationMs / 1000)
    : NaN;
  const blockMs = weightedRate(blocks, analysisTicks) / 1e6;
  const entityMs = weightedRate(entities, analysisTicks) / 1e6;

  console.log(`start       : ${diagnostics.start ? new Date(diagnostics.start).toISOString() : 'N/A'}`);
  console.log(`duration    : ${Number.isFinite(durationMs) ? durationMs : 'N/A'} ms`);
  console.log(`ticks       : ${analysisTicks || 'N/A'} (${tickSource})`);
  console.log(`effective TPS: ${fmt(effectiveTps)} (${tickSource})`);
  console.log(`tick period : ${analysisTicks && Number.isFinite(durationMs) ? fmt(durationMs / analysisTicks) : 'N/A'} ms (${tickSource})`);
  console.log(`data quality: ${JSON.stringify(issues)}`);
  console.log(`schema audit: ${JSON.stringify({
    unknownTopLevel: schema.unknownTopLevel,
    unknownData: schema.unknownData,
    unknownDiagnostics: schema.unknownDiagnostics,
    unknownAdditionalDiagnostics: schema.unknownAdditionalDiagnostics,
    unknownRowFields: schema.unknownRowFields,
    unknownPositionFields: schema.unknownPositionFields,
    unknownTraceFields: schema.unknownTraceFields,
    invalidTypes: schema.invalidTypes,
    invalidPositions: schema.invalidPositions,
    invalidTraceNodes: schema.invalidTraceNodes,
    invalidTraceChildren: schema.invalidTraceChildren,
    traceNodesInspected: schema.traceNodesInspected,
    traceInspectionLimited: schema.traceInspectionLimited,
  })}`);
  if (issues.numericStringRate || issues.numericStringTicks ||
      issues.fractionalTicks || issues.fractionalTotalTicks) {
    console.log(`numeric warnings: numericStringRate=${issues.numericStringRate} ` +
      `numericStringTicks=${issues.numericStringTicks} fractionalTicks=${issues.fractionalTicks} ` +
      `fractionalTotalTicks=${issues.fractionalTotalTicks}`);
  }
  console.log(`blocks      : ${blocks.length} entries, ${fmt(blockMs)} ms/tick weighted`);
  console.log(`entities    : ${entities.length} entries, ${fmt(entityMs)} ms/tick weighted`);
  printPerEntryTraceStats('blocks', blocks);
  printPerEntryTraceStats('entities', entities);
  printOverrange('blocks', blocks, analysisTicks);
  printOverrange('entities', entities, analysisTicks);
  console.log(`Observable  : ${diagnostics.observableVersion || 'N/A'}`);
  console.log(`Minecraft   : ${diagnostics.minecraftVersion || 'N/A'} (${diagnostics.modLoader || 'N/A'})`);

  for (const [label, rows, key] of [
    ['block groups', blocks, 'group'],
    ['entity groups', entities, 'group'],
    ['block namespace candidates', blocks, '__namespace'],
    ['entity namespace candidates', entities, '__namespace'],
    ['block types', blocks, 'type'],
    ['entity types', entities, 'type'],
  ]) {
    if (key === '__namespace') {
      for (const row of blocks.concat(entities)) row.__namespace = namespaceCandidate(row);
    }
    console.log(`\n${label}:`);
    for (const row of grouped(rows, key, analysisTicks).slice(0, 30)) {
      const suffix = key === '__namespace' ? ' [namespace candidate; ID prefix, not CPU owner]' : '';
      console.log(`  ${fmt(row.msPerTick)} ms/tick  entries=${row.entries} activeTicks=${row.activeTicks} max=${fmt(row.maxEntryMs)}ms  ${row.name}${suffix}`);
    }
  }

  console.log('\ntop entries:');
  for (const row of [...blocks, ...entities]
    .filter(row => row._rate !== null && row._ticks !== null && row._ticks <= analysisTicks)
    .sort((a, b) => ((b._rate ?? -1) * (b._ticks ?? -1)) -
                    ((a._rate ?? -1) * (a._ticks ?? -1)))
    .slice(0, 40)) {
    const ms = analysisTicks && row._rate !== null && row._ticks !== null
      ? row._rate * row._ticks / analysisTicks / 1e6
      : NaN;
    const pos = row.position ? ` @${row.position.x},${row.position.y},${row.position.z}` : '';
    console.log(`  ${fmt(ms)} ms/tick  rate=${row._rate === null ? 'N/A' : row._rate} ns/active-tick  ${row.group} ${row.type || '(unknown)'} ticks=${row.ticks ?? 'N/A'}${pos}`);
  }

  console.log('\nglobal trace methods (max node count, not time; parent/child counts are not additive):');
  const traceStats = globalTraceStats(value.data.traces);
  for (const [key, count] of [...traceStats.counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 60)) {
    console.log(`  max=${String(count).padStart(6)} ${key}`);
  }
  console.log('global trace exact paths (max node count):');
  for (const [key, count] of [...traceStats.pathCounts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20)) {
    console.log(`  max=${String(count).padStart(6)} ${key}`);
  }
  const limits = traceStats.limits;
  console.log(`global trace nodes=${limits.visited} maxDepth=${limits.maxDepth} ` +
    `truncated=${traceStats.truncated} depthLimited=${limits.depthLimited} nodeLimited=${limits.nodeLimited} ` +
    `malformedNodes=${limits.malformedNodes} malformedChildren=${limits.malformedChildren} ` +
    `rootInvalid=${limits.rootInvalid}`);
  console.log(`fingerprints: ${JSON.stringify(reportFingerprints(value))}`);
}

function probe(file) {
  const { value, raw } = readJson(file);
  const diagnostics = value.diagnostics ?? {};
  console.log(`file        : ${path.resolve(file)}`);
  console.log(`bytes       : ${raw.length}`);
  console.log(`sha256      : ${sha256(raw)}`);
  console.log(`diagnostic user present: ${Boolean(diagnostics.user)}`);
  summary(value);
}

function compare(fileA, fileB) {
  const a = readJson(fileA).value;
  const b = readJson(fileB).value;
  const issuesA = newIssueTracker();
  const issuesB = newIssueTracker();
  const ar = {
    blocks: allRows(a.data, 'blocks', issuesA),
    entities: allRows(a.data, 'entities', issuesA),
  };
  const br = {
    blocks: allRows(b.data, 'blocks', issuesB),
    entities: allRows(b.data, 'entities', issuesB),
  };
  const aTickInfo = resolveTicks(a, [...ar.blocks, ...ar.entities], issuesA);
  const bTickInfo = resolveTicks(b, [...br.blocks, ...br.entities], issuesB);
  const at = aTickInfo.ticks;
  const bt = bTickInfo.ticks;
  markOverrange(ar.blocks, at, issuesA);
  markOverrange(ar.entities, at, issuesA);
  markOverrange(br.blocks, bt, issuesB);
  markOverrange(br.entities, bt, issuesB);
  const aStart = Number(a.diagnostics.start);
  const bStart = Number(b.diagnostics.start);
  const aDuration = Number(a.diagnostics.duration);
  const bDuration = Number(b.diagnostics.duration);
  const aEnd = aStart + (Number.isFinite(aDuration) ? aDuration : 0);
  const bEnd = bStart + (Number.isFinite(bDuration) ? bDuration : 0);
  const gapMs = bStart >= aEnd ? bStart - aEnd : aStart >= bEnd ? aStart - bEnd : 0;
  const overlapMs = Math.max(0, Math.min(aEnd, bEnd) - Math.max(aStart, bStart));
  console.log(`A: [${Number.isFinite(aStart) ? new Date(aStart).toISOString() : 'N/A'}, ${Number.isFinite(aEnd) ? new Date(aEnd).toISOString() : 'N/A'}) ticks=${at || 'N/A'} (${aTickInfo.source})`);
  console.log(`B: [${Number.isFinite(bStart) ? new Date(bStart).toISOString() : 'N/A'}, ${Number.isFinite(bEnd) ? new Date(bEnd).toISOString() : 'N/A'}) ticks=${bt || 'N/A'} (${bTickInfo.source})`);
  console.log(`gap     : ${fmt(gapMs)} ms`);
  console.log(`overlap : ${fmt(overlapMs)} ms`);
  console.log(`union   : ${fmt(Math.max(aEnd, bEnd) - Math.min(aStart, bStart))} ms`);
  console.log(`same user: ${a.diagnostics.user && b.diagnostics.user ? a.diagnostics.user === b.diagnostics.user : 'unknown'}`);
  console.log(`same MC/loader: ${a.diagnostics.minecraftVersion === b.diagnostics.minecraftVersion && a.diagnostics.modLoader === b.diagnostics.modLoader}`);
  console.log(`same Observable version: ${a.diagnostics.observableVersion === b.diagnostics.observableVersion}`);
  const fingerprintsA = reportFingerprints(a);
  const fingerprintsB = reportFingerprints(b);
  console.log(`fingerprint A: ${JSON.stringify({
    mods: fingerprintsA.mods.sha256,
    system: fingerprintsA.system.sha256,
    environment: fingerprintsA.environment.sha256,
    schema: fingerprintsA.schema.sha256,
  })}`);
  console.log(`fingerprint B: ${JSON.stringify({
    mods: fingerprintsB.mods.sha256,
    system: fingerprintsB.system.sha256,
    environment: fingerprintsB.environment.sha256,
    schema: fingerprintsB.schema.sha256,
  })}`);
  console.log(`same Mods fingerprint: ${fingerprintsA.mods.present && fingerprintsB.mods.present ? fingerprintsA.mods.sha256 === fingerprintsB.mods.sha256 : 'unknown'}`);
  console.log(`same System Report fingerprint: ${fingerprintsA.system.present && fingerprintsB.system.present ? fingerprintsA.system.sha256 === fingerprintsB.system.sha256 : 'unknown'}`);
  console.log(`same environment fingerprint: ${fingerprintsA.environment.present && fingerprintsB.environment.present ? fingerprintsA.environment.sha256 === fingerprintsB.environment.sha256 : 'unknown'}`);
  console.log(`same schema fingerprint: ${fingerprintsA.schema.sha256 === fingerprintsB.schema.sha256}`);
  if (fingerprintsA.mods.present && fingerprintsB.mods.present && fingerprintsA.mods.sha256 !== fingerprintsB.mods.sha256) {
    console.log('  warning: Mods lists differ; cross-window persistence is not a clean same-instance comparison');
  }
  if (fingerprintsA.system.present && fingerprintsB.system.present && fingerprintsA.system.sha256 !== fingerprintsB.system.sha256) {
    console.log('  warning: System Report differs; CPU/JVM/OS or runtime resource conditions may have changed');
  }
  if (fingerprintsA.schema.sha256 !== fingerprintsB.schema.sha256) {
    console.log('  warning: schema fingerprints differ; compare fields and traces conservatively');
  }
  console.log(`data quality A: ${JSON.stringify(issuesA)}`);
  console.log(`data quality B: ${JSON.stringify(issuesB)}`);
  console.log(`schema audit A: ${JSON.stringify(schemaAudit(a))}`);
  console.log(`schema audit B: ${JSON.stringify(schemaAudit(b))}`);
  printOverrange('A blocks', ar.blocks, at);
  printOverrange('A entities', ar.entities, at);
  printOverrange('B blocks', br.blocks, bt);
  printOverrange('B entities', br.entities, bt);

  for (const [label, key] of [['blocks', 'blocks'], ['entities', 'entities']]) {
    const ag = new Map(grouped(ar[key], 'group', at).map(row => [row.name, row.msPerTick]));
    const bg = new Map(grouped(br[key], 'group', bt).map(row => [row.name, row.msPerTick]));
    console.log(`\n${label} group deltas (B - A ms/tick):`);
    for (const name of new Set([...ag.keys(), ...bg.keys()])) {
      const delta = (bg.get(name) || 0) - (ag.get(name) || 0);
      console.log(`  ${fmt(delta)} ${name} (A=${fmt(ag.get(name) || 0)}, B=${fmt(bg.get(name) || 0)})`);
    }
    const aKeys = new Set(ar[key].map(rowKey));
    const bKeys = new Set(br[key].map(rowKey));
    const added = [...bKeys].filter(keyValue => !aKeys.has(keyValue)).length;
    const removed = [...aKeys].filter(keyValue => !bKeys.has(keyValue)).length;
    console.log(`${label} object keys: A=${aKeys.size} B=${bKeys.size} added=${added} removed=${removed}`);
    const namespaceA = new Map(grouped(ar[key].map(row => ({ ...row, __namespace: namespaceCandidate(row) })), '__namespace', at)
      .map(row => [row.name, row.msPerTick]));
    const namespaceB = new Map(grouped(br[key].map(row => ({ ...row, __namespace: namespaceCandidate(row) })), '__namespace', bt)
      .map(row => [row.name, row.msPerTick]));
    console.log(`${label} namespace candidates (B - A ms/tick; ID prefix, not CPU owner):`);
    for (const name of new Set([...namespaceA.keys(), ...namespaceB.keys()])) {
      const delta = (namespaceB.get(name) || 0) - (namespaceA.get(name) || 0);
      console.log(`  ${fmt(delta)} ${name} (A=${fmt(namespaceA.get(name) || 0)}, B=${fmt(namespaceB.get(name) || 0)})`);
    }
    if (key === 'entities') printEntityChurn(label, ar[key], br[key]);
  }
  printPerEntryTraceStats('A blocks', ar.blocks);
  printPerEntryTraceStats('B blocks', br.blocks);
  printPerEntryTraceStats('A entities', ar.entities);
  printPerEntryTraceStats('B entities', br.entities);
}

function align(sparkFile, observableFile) {
  const spark = readAnyJson(sparkFile);
  const observable = readJson(observableFile).value;
  const meta = spark.metadata || spark;
  let sparkStart = Number(meta.start_time ?? meta.start ?? meta.sampling?.start_time);
  let sparkEnd = Number(meta.end_time ?? meta.end);
  if ((!Number.isFinite(sparkStart) || !Number.isFinite(sparkEnd)) && Array.isArray(spark.perWindow) && spark.perWindow.length) {
    const windows = spark.perWindow
      .map((w) => ({ start: Number(w.start_time), duration: Number(w.duration_ms) }))
      .filter((w) => Number.isFinite(w.start) && Number.isFinite(w.duration));
    if (windows.length) {
      sparkStart = Math.min(...windows.map((w) => w.start));
      sparkEnd = Math.max(...windows.map((w) => w.start + w.duration));
    }
  }
  if (!Number.isFinite(sparkEnd) && Number.isFinite(sparkStart) && Number.isFinite(meta.sampling?.seconds)) {
    sparkEnd = sparkStart + meta.sampling.seconds * 1000;
  }
  const obsStart = Number(observable.diagnostics?.start);
  const obsDuration = Number(observable.diagnostics?.duration);
  const obsEnd = obsStart + (Number.isFinite(obsDuration) ? obsDuration : 0);
  const overlap = Number.isFinite(sparkStart) && Number.isFinite(sparkEnd) && Number.isFinite(obsStart)
    ? Math.max(0, Math.min(sparkEnd, obsEnd) - Math.max(sparkStart, obsStart)) : 0;
  const gap = Number.isFinite(sparkStart) && Number.isFinite(sparkEnd) && Number.isFinite(obsStart)
    ? (obsStart >= sparkEnd ? obsStart - sparkEnd : sparkStart >= obsEnd ? sparkStart - obsEnd : 0) : NaN;
  const containment = Number.isFinite(sparkStart) && Number.isFinite(sparkEnd) && Number.isFinite(obsStart)
    ? (sparkStart <= obsStart && obsEnd <= sparkEnd ? 'observable_in_spark' : obsStart <= sparkStart && sparkEnd <= obsEnd ? 'spark_in_observable' : overlap > 0 ? 'partial' : 'none')
    : 'unknown';
  const obsTicks = Number(observable.data?.ticks);
  console.log(`spark    : [${Number.isFinite(sparkStart) ? new Date(sparkStart).toISOString() : 'N/A'}, ${Number.isFinite(sparkEnd) ? new Date(sparkEnd).toISOString() : 'N/A'})`);
  console.log(`observable: [${Number.isFinite(obsStart) ? new Date(obsStart).toISOString() : 'N/A'}, ${Number.isFinite(obsEnd) ? new Date(obsEnd).toISOString() : 'N/A'}) ticks=${Number.isFinite(obsTicks) ? obsTicks : 'N/A'} (${Number.isFinite(obsTicks) && obsTicks > 0 ? 'observed' : 'inferred/missing'})`);
  console.log(`gap=${fmt(gap)} ms overlap=${fmt(overlap)} ms containment=${containment} union=${fmt(Math.max(sparkEnd, obsEnd) - Math.min(sparkStart, obsStart))} ms`);
  console.log(`spark aggregator=${JSON.stringify(meta.data_aggregator || spark.aggregator || null)} threadDumper=${JSON.stringify(meta.thread_dumper || spark.threadDumper || null)}`);
  console.log('denominators: Spark and Observable remain separate; do not add MSPT, owner/self, object weighted ms/tick, or trace counts.');
  console.log(`evidence: ${overlap > 0 ? 'L3 candidate for same-window path cross-check' : 'L1/L2 only; no same-window cross-check'}`);
}

async function fetchReport(input, outFile) {
  const endpoint = endpointFromInput(input);
  const response = await fetch(endpoint);
  const bytes = Buffer.from(await response.arrayBuffer());
  if (!response.ok) throw new Error(`HTTP ${response.status} from ${endpoint}`);
  const contentType = response.headers.get('content-type') || '';
  if (!contentType.toLowerCase().includes('json')) {
    throw new Error(`response from ${endpoint} has non-JSON content-type ${contentType || '(missing)'}`);
  }
  const text = bytes.toString('utf8');
  let value;
  try {
    value = JSON.parse(text);
  } catch {
    throw new Error(`response from ${endpoint} is not JSON (possibly viewer HTML)`);
  }
  validatePayload(value, endpoint);
  fs.mkdirSync(path.dirname(path.resolve(outFile)), { recursive: true });
  fs.writeFileSync(outFile, bytes);
  const source = {
    input,
    endpoint,
    fetchedAt: new Date().toISOString(),
    bytes: bytes.length,
    sha256: sha256(bytes),
  };
  fs.writeFileSync(`${outFile}.source.json`, `${JSON.stringify(source, null, 2)}\n`);
  console.log(`saved ${outFile} (${bytes.length} bytes)`);
  console.log(`source metadata ${outFile}.source.json`);
  console.log(`sha256 ${source.sha256}`);
}

const [command, ...args] = process.argv.slice(2);
try {
  if (command === 'fetch') {
    const input = args[0];
    const outIndex = args.indexOf('--out');
    const outFile = outIndex >= 0 ? args[outIndex + 1] : null;
    if (!input || !outFile) throw new Error('fetch requires an input and --out <file.json>');
    await fetchReport(input, outFile);
  } else if (command === 'probe') {
    if (!args[0]) throw new Error('probe requires <file.json>');
    probe(args[0]);
  } else if (command === 'summarize') {
    if (!args[0]) throw new Error('summarize requires <file.json>');
    summary(readJson(args[0]).value);
  } else if (command === 'compare') {
    if (!args[0] || !args[1]) throw new Error('compare requires <file-a.json> <file-b.json>');
    compare(args[0], args[1]);
  } else if (command === 'align' || command === 'joint') {
    if (!args[0] || !args[1]) throw new Error(`${command} requires <spark-probe-or-analysis.json> <observable.json>`);
    align(args[0], args[1]);
  } else {
    fail('unknown command');
  }
} catch (error) {
  fail(error.message);
}

