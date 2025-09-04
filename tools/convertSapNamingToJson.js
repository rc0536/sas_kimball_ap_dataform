#!/usr/bin/env node
/*
 * convertSapNamingToJson.js
 *
 * This script converts an export of the SAP_Naming table into
 * per‑table JSON metadata files.  It expects a tab‑ or comma‑delimited
 * file with at least the following columns (case‑insensitive):
 *   Table, Field, SasName, Datatype, Length, Decimals
 *
 * Usage: node convertSapNamingToJson.js <input.tsv|csv> <outputDir>
 *
 * Example: node convertSapNamingToJson.js sample_sap_naming.tsv includes/metadata
 */
const fs = require('fs');
const path = require('path');

if (process.argv.length < 4) {
  console.error('Usage: node convertSapNamingToJson.js <input.tsv|csv> <outputDir>');
  process.exit(1);
}

const inputPath = process.argv[2];
const outputDir = process.argv[3];
const content = fs.readFileSync(inputPath, 'utf8');

// Determine delimiter
const delim = content.includes('\t') ? '\t' : ',';

const lines = content.split(/\r?\n/).filter(Boolean);
const header = lines[0].split(delim).map(h => h.trim());
const rows = lines.slice(1).map(line => {
  const parts = line.split(delim).map(s => s.trim());
  const obj = {};
  header.forEach((h, idx) => {
    obj[h] = parts[idx] || '';
  });
  return obj;
});

const byTable = {};
rows.forEach(r => {
  const table = (r.Table || r.TABLE || r.table || '').trim();
  if (!table) return;
  if (!byTable[table]) byTable[table] = [];
  byTable[table].push({
    Table: table,
    Field: (r.Field || r.FIELD || r.field || '').trim(),
    SasName: (r.SasName || r.SASNAME || r.sasname || '').trim(),
    Datatype: (r.Datatype || r.DATATYPE || r.datatype || r.SasDatatype || '').trim(),
    Length: (r.Length || r.LENGTH || r.length || '').trim(),
    Decimals: (r.Decimals || r.DECIMALS || r.decimals || '').trim()
  });
});

fs.mkdirSync(outputDir, { recursive: true });
Object.entries(byTable).forEach(([tableName, fields]) => {
  // sort by field to maintain stable ordering (if Ordinal exists, use it)
  const sorted = fields.sort((a, b) => a.Field.localeCompare(b.Field));
  const outPath = path.join(outputDir, tableName.toLowerCase() + '.json');
  fs.writeFileSync(outPath, JSON.stringify(sorted, null, 2));
  console.log(`Wrote ${outPath} (${sorted.length} columns)`);
});
