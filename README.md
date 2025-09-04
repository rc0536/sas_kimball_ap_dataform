# AP SAP Dataform Repository

This repository converts SAP staging tables (prefixed with `stg_`) into curated target tables and views suitable for analytics and downstream processing.  It mirrors the behaviour of a legacy SQL Server procedure that generated target tables via dynamic SQL, including:

* Mapping each raw column to a friendly `SasName` alias based on the `SAP_Naming` metadata.
* Concatenating the underlying field name to the alias when multiple source fields share the same `SasName` (e.g. `AmtInForeignCurrForTaxBreakdown_WRBT1`).
* Casting values into appropriate BigQuery types: numeric, decimal with scale 2, date, datetime, etc., with sensible fall‑backs for invalid values (see `includes/warehouses/bigquery.js`).
* Appending two additional columns to every target table:
  - `SAS_IMPORT_DATETIME` — defaults to the current timestamp when the row is materialised.
  - `SASREFNBR` — a surrogate key.  By default this is generated via `GENERATE_UUID()`, but it can be switched to a deterministic hash via the `surrogateKeyStrategy` variable in `dataform.json`.

## Project structure

```
dataform/
├─ dataform.json           # Dataform project configuration
├─ package.json            # Basic Node package manifest (no dependencies needed)
├─ README.md               # This file
├─ includes/
│  ├─ lib.js               # Core helpers for loading metadata and building SELECT lists
│  ├─ warehouses/
│  │  └─ bigquery.js       # Type‑casting rules for BigQuery (safe casts)
│  └─ metadata/
│     ├─ bsak.json         # Partial SAP_Naming metadata for BSAK (extend as needed)
│     ├─ bsik.json         # Partial SAP_Naming metadata for BSIK
│     ├─ lfa1.json         # Partial SAP_Naming metadata for LFA1
│     ├─ lfb1.json         # Partial SAP_Naming metadata for LFB1
│     ├─ lfm1.json         # Partial SAP_Naming metadata for LFM1
│     ├─ payr.json         # Partial SAP_Naming metadata for PAYR
│     ├─ reguh.json        # Partial SAP_Naming metadata for REGUH
│     ├─ bvor.json         # Partial SAP_Naming metadata for BVOR
│     └─ adr6.json         # Partial SAP_Naming metadata for ADR6
├─ definitions/
│  ├─ staging/
│  │  └─ sources.sqlx      # Declarations for the raw staging tables
│  ├─ targets/
│  │  ├─ bsak.sqlx         # Materialises the curated BSAK table
│  │  ├─ bsik.sqlx         # Materialises the curated BSIK table
│  │  ├─ lfa1.sqlx         # Materialises the curated LFA1 table
│  │  ├─ lfb1.sqlx         # Materialises the curated LFB1 table
│  │  ├─ lfm1.sqlx         # Materialises the curated LFM1 table
│  │  ├─ payr.sqlx         # Materialises the curated PAYR table
│  │  ├─ reguh.sqlx        # Materialises the curated REGUH table
│  │  ├─ bvor.sqlx         # Materialises the curated BVOR table
│  │  └─ adr6.sqlx         # Materialises the curated ADR6 table
│  └─ ap_models/
│     ├─ ap_bsak_enriched.sqlx  # Example view joining BSAK with vendor master data
│     └─ ap_bsik_enriched.sqlx  # Example view joining BSIK with vendor master data
└─ tools/
   ├─ convertSapNamingToJson.js # Helper to convert an SAP_Naming TSV into JSON files
   └─ sample_sap_naming.tsv     # Placeholder for the raw SAP_Naming export (paste your data here)
```

## Usage

1. **Create staging tables.**  Make sure your BigQuery project has raw staging tables such as `staging.stg_bsak`, `staging.stg_bsik` and so on.

2. **Load metadata.**  Place a TSV or CSV export of your `SAP_Naming` metadata (with columns `Table,Ordinal,Field,SasName,Description,Element,Checktable,Datatype,Length,Decimals,SasDatatype`) into `tools/sample_sap_naming.tsv` and run:

```bash
node tools/convertSapNamingToJson.js tools/sample_sap_naming.tsv includes/metadata
```

This will overwrite the partial JSON files in `includes/metadata` with complete mappings for each table.

3. **Compile & run** in Dataform (locally or via Cloud Dataform).  Each `targets/*.sqlx` file will build a table in the schema defined in `dataform.json` (`ap` by default).  The `ap_models/` files show how to create reporting views joining the curated data with the vendor master.

## Extending the metadata

The JSON files under `includes/metadata` currently contain a minimal subset of columns for brevity.  To match the full functionality of your SQL conversion:

* Paste your complete `SAP_Naming` export into `tools/sample_sap_naming.tsv`.
* Run `node tools/convertSapNamingToJson.js` as shown above.
* Dataform will automatically pick up all columns, handle duplicates, and cast types accordingly.

You can modify the casting behaviour in `includes/warehouses/bigquery.js` if you need to tune the numeric precision or date parsing logic.