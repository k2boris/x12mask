# X12 837P Masking Pipeline

Minimal Java pipeline for transforming X12 837P claims into canonical/mapping data, staging in SQLite, reading masked data from K2view Fabric, and regenerating masked X12 output.

## Table of Contents
1. [What This Project Does](#what-this-project-does)
2. [Repository Layout](#repository-layout)
3. [Prerequisites](#prerequisites)
4. [Configuration](#configuration)
5. [How to Run](#how-to-run)
6. [Examples](#examples)
7. [Specifications](#specifications)

## What This Project Does
1. Parses X12 837P files from `data/in`.
2. Extracts canonical (`biz_*`) and mapping (`map_*`) datasets.
3. Validates round-trip structure before staging.
4. Stages extracted data into local SQLite (`connector/x12-staging.db`).
5. Reads masked data through K2view JDBC for a selected `transaction_id`.
6. Reconstructs masked X12 and writes outputs to `data/out`.

## Repository Layout
- `src/`: Java source code
- `data/in`: Input X12 files
- `data/trace`: Extracted/masked traces + reconstruction reports + transaction context
- `data/out`: Masked X12 output
- `connector/`: SQLite staging DB and schema
- `jars/`: Runtime JDBC jars (`fabric-jdbc`, `sqlite-jdbc`)
- `spec/`: Technical specifications
- `00_compile.sh`: Compile helper
- `01_process.sh`: Stage 1 (process + optional docker copy)
- `02_mask.sh`: Stage 2 (mask by `transaction_id`)
- `03_clear.sh`: Full local reset + optional docker copy

## Prerequisites
1. Java installed (`javac`, `java`).
2. JDBC jars in `jars/`:
   - Fabric JDBC driver
   - SQLite JDBC driver (Apple Silicon-compatible if on arm64 Mac).
3. Optional: Docker (for copying staging DB into local K2view container).

## Configuration
Main config file: `application.properties`.

Key settings:
1. Pipeline/data paths: `app.input.dir`, `app.output.dir`, `app.trace.dir`, `app.connector.dir`
2. Stage transaction sequence: `app.transaction.start`
3. SQLite staging: `app.sqlite.jdbc.url`, `app.sqlite.schema.file`
4. K2view JDBC: `app.jdbc.url`, `app.jdbc.session.init.template`, `app.read.tables`
5. Optional Docker sync target:
   - `app.docker.k2.container`
   - `app.docker.k2.db.dest`

## How to Run
From repository root:

```bash
./00_compile.sh
./01_process.sh
./02_mask.sh TXN-1002
```

Reset local state:

```bash
./03_clear.sh
```

## Examples
After `./01_process.sh` with two files in `data/in`, typical generated transaction IDs:
1. `TXN-1002`
2. `TXN-1003`

Mask a specific transaction:

```bash
./02_mask.sh TXN-1003
```

Expected artifacts:
1. `data/trace/<root>.extracted.json`
2. `data/trace/<root>.masked.json`
3. `data/trace/<root>.reconstruct.trace.txt`
4. `data/out/<root>.masked.x12`
5. `data/trace/<transaction_id>.context.properties`

## Specifications
1. App technical spec:
   - `spec/X12_837P_Masking_Application_Technical_Spec_v1.docx`
2. Canonical/mapping technical spec:
   - `spec/X12_837P_Canonical_and_Mapping_Technical_Spec_v1.docx`
