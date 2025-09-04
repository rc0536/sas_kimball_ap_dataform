/*
 * BigQuery casting and typing helpers.
 *
 * This module defines two functions used by the repository:
 *
 *  - `castBigQuery`: takes a source expression (`src`), plus the SAP datatype,
 *    length, and decimals metadata, and returns a SQL expression that safely
 *    casts the value into a BigQuery type.  It mirrors the defensive logic
 *    from the T‑SQL solution: default 0 for integers, 1900‑01‑01 for bad dates,
 *    `LEFT(.., 15)` behaviour for currency fields, etc.
 *
 *  - `targetTypeBigQuery`: returns the BigQuery type string used when
 *    generating DDL statements (for documentation purposes).  It does not
 *    control casting; it merely annotates the expected type of each column.
 */

/**
 * Safely cast a column expression to a target type.
 *
 * @param {Object} opts
 * @param {string} opts.src      The source column expression (quoted).
 * @param {string} opts.sapDatatype The SAP datatype (e.g. NUMC, CURR, DATS).
 * @param {number} opts.length   The declared length of the field.
 * @param {number} opts.decimals The number of decimal places (for CURR/DEC).  Optional.
 * @returns {string} The SQL expression to cast the column.
 */
export function castBigQuery({ src, sapDatatype, length, decimals }) {
  const trimmed = `TRIM(${src})`;
  switch ((sapDatatype || '').toUpperCase()) {
    case 'NUMC':
    case 'INT4':
      // Default to 0 when blank or non‑numeric
      return `COALESCE(SAFE_CAST(NULLIF(${trimmed}, '') AS INT64), 0)`;

    case 'CURR':
      // Remove commas, take first 15 characters, cast to NUMERIC, round to 2 decimals
      return `ROUND(SAFE_CAST(REGEXP_REPLACE(SUBSTR(${trimmed}, 1, 15), r',', '') AS NUMERIC), 2)`;

    case 'DEC':
      // Remove commas, cast to NUMERIC and round to provided decimals (default 0)
      return `ROUND(SAFE_CAST(REGEXP_REPLACE(${trimmed}, r',', '') AS NUMERIC), ${decimals || 0})`;

    case 'DATS':
      // Parse YYYYMMDD; fall back to 1900‑01‑01
      return `COALESCE(
        SAFE.PARSE_DATE('%Y%m%d', ${trimmed}),
        DATE '1900-01-01'
      )`;

    case 'TIMS':
      // Keep as string for now (HHMMSS)
      return `${src}`;

    case 'CLNT':
    case 'CHAR':
    case 'CUKY':
    case 'LANG':
    case 'RAW':
    default:
      // Default to string
      return `${src}`;
  }
}

/**
 * Derive an expected BigQuery column type from SAP metadata.
 *
 * This is used for informational DDL generation.  It does not affect
 * casting behaviour; see `castBigQuery` for runtime casting.
 *
 * @param {Object} opts
 * @param {string} opts.sapDatatype The SAP datatype (NUMC, CURR, etc.)
 * @param {number} opts.length   The declared length (ignored in BigQuery)
 * @param {number} opts.decimals The number of decimals
 * @returns {string} A BigQuery type string (e.g. INT64, NUMERIC, DATE, STRING).
 */
export function targetTypeBigQuery({ sapDatatype, length, decimals }) {
  switch ((sapDatatype || '').toUpperCase()) {
    case 'NUMC':
    case 'INT4':
      return 'INT64';
    case 'CURR':
    case 'DEC':
      return 'NUMERIC';
    case 'DATS':
      return 'DATE';
    case 'TIMS':
      return 'STRING';
    default:
      return 'STRING';
  }
}