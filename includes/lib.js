/*
 * Core helper functions for the Dataform SAP project.
 *
 * This file exposes utilities for loading table metadata (in JSON
 * generated from the SAP_Naming table) and building SELECT lists
 * that rename and cast columns while dealing with duplicate friendly
 * names.  It also offers a helper to render DDL definitions purely
 * for documentation or debugging.
 */

import { castBigQuery, targetTypeBigQuery } from './warehouses/bigquery.js';

/**
 * Load metadata from a JSON file in includes/metadata.  Each entry
 * should contain at least `Table`, `Field`, `SasName`, `Datatype`,
 * `Length`, and `Decimals` properties.  Use this function in your
 * SQLX scripts to import the metadata at compile time.
 *
 * @param {string} jsonPath Path to the metadata JSON.  Use a relative
 *   import like `includes/metadata/bsak.json`.  Dataform's require
 *   resolves relative to the project root.
 * @returns {Array<Object>} Normalised metadata entries.
 */
export function loadMeta(jsonPath) {
  // eslint-disable-next-line global-require, import/no-dynamic-require
  const raw = require(jsonPath);
  return raw.map((r) => ({
    Table: (r.Table || r.TABLE || r.table || '').trim(),
    Field: (r.Field || r.FIELD || r.field || '').trim(),
    SasName: (r.SasName || r.SASNAME || r.sasname || '').trim(),
    Datatype: (r.Datatype || r.DATATYPE || r.datatype || '').trim(),
    Length: Number(r.Length || r.length || r.LengthChars || 0) || 0,
    Decimals: Number(r.Decimals || r.decimals || r.Scale || 0) || 0,
  }));
}

/**
 * Build a SELECT list from table metadata.  For each entry in
 * `meta`, this helper produces a line of SQL that casts and aliases
 * the source field.  If multiple metadata rows share the same
 * `SasName`, the field name is appended (e.g. `SasName_Field`).
 *
 * Two extra columns are appended at the end:
 *
 *   - `SAS_IMPORT_DATETIME` — always `CURRENT_TIMESTAMP()`.
 *   - `SASREFNBR` — a surrogate key.  Determined by
 *     `surrogateKeyStrategy`: either a UUID or a SHA256 hash of all
 *     input fields (concatenated).
 *
 * @param {Object} params
 * @param {Array<Object>} params.meta Metadata entries for a table.
 * @param {string} params.sourceAlias The alias of the source table in the SQL.
 * @param {string} params.surrogateKeyStrategy Either `uuid` (default) or `hash`.
 * @returns {string} A comma‑delimited SELECT list.
 */
export function buildSelect({ meta, sourceAlias, surrogateKeyStrategy = 'uuid' }) {
  // Count occurrences of each SasName to detect duplicates
  const counts = meta.reduce((acc, r) => {
    const name = (r.SasName || r.Field || '').trim();
    acc[name] = (acc[name] || 0) + 1;
    return acc;
  }, {});

  // Build each SELECT expression
  const lines = meta.map((r) => {
    const field = r.Field;
    const sas = r.SasName || field;
    const targetName = counts[sas] > 1 ? `${sas}_${field}` : sas;
    const src = `\`${sourceAlias}\`.\`${field}\``;
    const expr = castBigQuery({ src, sapDatatype: r.Datatype, length: r.Length, decimals: r.Decimals });
    return `  ${expr} AS \`${targetName}\``;
  });

  // Append SAS_IMPORT_DATETIME
  lines.push('  CURRENT_TIMESTAMP() AS `SAS_IMPORT_DATETIME`');

  // Append surrogate key
  if (surrogateKeyStrategy === 'hash') {
    // Concatenate all source fields and hash them
    const concatExpr = meta.map((r) => `COALESCE(CAST(\`${sourceAlias}\`.\`${r.Field}\` AS STRING), '')`).join(', ');
    lines.push(`  TO_HEX(SHA256(CONCAT(${concatExpr}))) AS \`SASREFNBR\``);
  } else {
    lines.push('  GENERATE_UUID() AS `SASREFNBR`');
  }

  return lines.join(',\n');
}

/**
 * Render a BigQuery DDL definition for documentation.  This is not
 * used by Dataform directly but may be invoked to inspect the
 * expected types of your curated tables.
 *
 * @param {Object} params
 * @param {Array<Object>} params.meta Metadata entries
 * @returns {string} A parenthesised list of column definitions
 */
export function renderDDL({ meta }) {
  const counts = meta.reduce((acc, r) => {
    const name = (r.SasName || r.Field || '').trim();
    acc[name] = (acc[name] || 0) + 1;
    return acc;
  }, {});
  const cols = meta.map((r) => {
    const field = r.Field;
    const sas = r.SasName || field;
    const targetName = counts[sas] > 1 ? `${sas}_${field}` : sas;
    const type = targetTypeBigQuery({ sapDatatype: r.Datatype, length: r.Length, decimals: r.Decimals });
    return `  \`${targetName}\` ${type}`;
  });
  cols.push('  `SAS_IMPORT_DATETIME` TIMESTAMP');
  cols.push('  `SASREFNBR` STRING');
  return `(\n${cols.join(',\n')}\n)`;
}