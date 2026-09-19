# Azure— NYPA Energy Data Platform

Medallion-architecture pipeline on Azure, built around three real **New York Power
Authority (NYPA)** open datasets pulled through three different ingestion patterns —
file, database, and REST API — landing in a star-schema gold layer. End-to-end,
verified working: bronze ingestion through gold Delta tables.

## Sources

| Source type | Dataset | Ingestion pattern |
|---|---|---|
| CSV (file drop) | [NYPA Net Generation (MWh) by Facility](https://data.ny.gov/d/isux-jnrn) | Dropped into the `raw-csv` container, ADF Copy → bronze parquet |
| Database | [NYPA Electric Supply Rates — Governmental Entities](https://data.ny.gov/d/tj6m-a24c) | Seeded into Azure SQL DB (`dbo.governmental_rates`), incremental ADF copy watermarked on `data_as_of` |
| REST API | [NYPA Electric Supply Rates — Business Customers](https://data.ny.gov/d/2x8p-pewm) | Pulled live from the Socrata SODA API via an ADF REST linked service |

The two rate datasets share a shape (rate by customer segment, effective date) despite
coming from different technical sources — a deliberate design so silver/gold treat
them as parallel feeds into one conformed `fact_supply_rate`, while generation data
feeds a separate `fact_generation`.

## Architecture

```
CSV file  ──(ADF Copy)────────────────┐
Azure SQL ──(ADF incremental copy)────┤──▶ Bronze (ADLS Gen2, date-partitioned parquet/json)
Socrata REST API ──(ADF Copy)─────────┘         │
                                                 ▼
                              Databricks Silver (serverless, cleansed/conformed)
                                                 │
                                                 ▼
                              Databricks Gold (serverless, Delta star schema)
                    dim_facility · dim_date · dim_customer_type
                    fact_generation · fact_supply_rate
```

Secrets (SQL admin password, storage account key) live in Azure Key Vault — ADF's SQL
linked service resolves the password via an `AzureKeyVaultSecret` reference. Databricks
storage access goes through **Unity Catalog** (an access connector + storage credential
+ external locations, all managed-identity based) rather than a raw key. Nothing
sensitive is in this repo.

## Why serverless compute, not classic clusters

This subscription has a default **4 total vCPU** compute quota per region — a
restriction Azure applies to new/unverified subscriptions. Every classic
(VM-based) Databricks cluster we tried — `Standard_DS3_v2`, `D4s_v3`, `F4s_v2`,
`D4_v2`, even a 2-vCPU `D2ds_v6` — stalled or hit `SkuNotAvailable`/quota-adjacent
failures regardless of VM size or region (tried both `eastus` and `centralus`).

**Databricks serverless compute sidesteps this entirely**, since Databricks manages
that capacity pool itself rather than provisioning VMs in your subscription. The
tradeoff: serverless can't set `fs.azure.account.key` the way a classic cluster can,
so storage access has to be governed through Unity Catalog instead — which is also
just better practice than a shared key. See `scripts/setup_unity_catalog.sh` for the
one-time setup (also captured as Bicep resources in `infra/main.bicep`).

If you have (or later obtain) a higher vCPU quota, classic clusters work fine too —
just add a `job_clusters` block back to the job and point tasks at it.

## Deployed resources (resource group `nypa-de2-rg`)

| Resource | Name | Notes |
|---|---|---|
| Storage (ADLS Gen2) | `nypade2dls...` | Containers: `raw-csv`, `bronze`, `silver`, `gold` |
| Data Factory | `nypade2-adf` | 4 linked services, 7 datasets, 3 bronze pipelines |
| Azure SQL DB | `nypade2-sql3-...` | In `centralus` — `eastus`/`eastus2`/`westus2`/`southcentralus` all rejected new SQL server creation on this subscription at deploy time |
| Databricks workspace | `nypade2-dbx` | In `centralus` (moved from `eastus` — see quota note above). Notebooks under `/Shared/nypa_pipeline`; job `nypa_silver_gold_pipeline` runs silver → gold on serverless compute |
| Key Vault | `nypade2-kv-...` | `sql-admin-password`, `storage-account-key` secrets |
| Databricks access connector | `nypade2-uc-connector` | Managed identity backing the Unity Catalog storage credential |

## ADF pipelines

| Pipeline | Pattern |
|---|---|
| `pl_bronze_generation_csv` | Simple copy, raw-csv → bronze/generation |
| `pl_bronze_governmental_rates` | Lookup watermark → incremental copy (`data_as_of > watermark`) → update watermark (Script activity) |
| `pl_bronze_business_rates_api` | REST source (Socrata SODA API) → bronze/business_rates |

Bronze paths are date-partitioned (`.../ingest_date=yyyy-MM-dd/...`) so re-runs don't
clobber prior loads.

![pl_bronze_governmental_rates pipeline canvas — Lookup, Copy data, Script activities](pics/ingestion.jpg)

## Databricks notebooks (`workspace/`, mirrored to `/Shared/nypa_pipeline`)

| Notebook | Output |
|---|---|
| `silver_generation.py` | Cleansed generation, written as Delta |
| `silver_rates.py` | Governmental + business rates unioned into one conformed Delta table |
| `gold_dim_facility.py` | `dim_facility` — surrogate key per NYPA facility |
| `gold_dim_date.py` | `dim_date` — daily calendar, 2012–present |
| `gold_dim_customer_type.py` | `dim_customer_type` — governmental vs. business segments |
| `gold_fact_generation.py` | `fact_generation` — MWh by facility/year |
| `gold_fact_supply_rate.py` | `fact_supply_rate` — rates from both source systems, one fact table |

All read/write Delta (not raw parquet) between layers. They run as a single
Databricks Job (`nypa_silver_gold_pipeline`) with task dependencies matching the
medallion order, on serverless compute (no cluster spec needed).

![Deployed notebooks in /Shared/nypa_pipeline](pics/databricks_transform.jpg)

## Azure DevOps CI/CD

A companion repo, [`nypa-cicd-demo`](https://dev.azure.com/pankur715/pankur715/_git/pankur715),
demonstrates a real feature-branch → PR → main CI/CD workflow in Azure DevOps,
gated by a pipeline that validates and deploys a small monitoring add-on (a Log
Analytics workspace, `nypade2-logs`) into this project's actual `nypa-de2-rg`
resource group — not a mocked target.

```
feature/add-resource-tags ──(push)──▶ PR into main ──▶ CI: Validate stage
                                                          │  az bicep build
                                                          │  az deployment group validate
                                                          │  (dry run against nypa-de2-rg)
                                                          ▼
                                                     PR merged into main
                                                          │
                                                          ▼
                                                 CD: Deploy stage
                                                 az deployment group create
                                                 (real deploy to nypa-de2-rg)
```

### Setup

| Component | Detail |
|---|---|
| Organization / Project | `pankur715` |
| Repo | Azure Repos Git, `pankur715` — separate from this GitHub repo by design (Azure DevOps and GitHub are different services; this keeps the CI/CD demo self-contained) |
| Service connection | `nypa-arm-connection` — Azure Resource Manager, scoped to the `nypa-de2-rg` resource group only (least privilege — it can't touch anything else in the subscription), created via the ADO portal's automatic service-principal flow so no secret ever passed through this session |
| Pipeline | `nypa-cicd-demo`, defined by `azure-pipelines.yml` in the companion repo |

### Pipeline definition

```yaml
trigger:
  branches:
    include: [main]

pr:
  branches:
    include: [main]

pool: 'Default'   # self-hosted — see "New-org compute quota" below

stages:
  - stage: Validate            # runs on every PR targeting main
      - az bicep build infra/monitoring.bicep
      - az deployment group validate -g nypa-de2-rg --template-file infra/monitoring.bicep

  - stage: Deploy               # runs only on main, after a PR merges
    dependsOn: Validate
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
      - az deployment group create -g nypa-de2-rg --template-file infra/monitoring.bicep
```

### What was actually run

1. Pushed `main` with the initial `infra/monitoring.bicep` + `azure-pipelines.yml`.
2. Created `feature/add-resource-tags`, added `tags` (project/managedBy) to the
   Log Analytics workspace resource, pushed the branch.
3. Opened PR #1 (`feature/add-resource-tags` → `main`) via `az repos pr create`.
4. **Validate** stage ran against the real `nypa-de2-rg` resource group —
   `az bicep build` compiled the template, `az deployment group validate`
   confirmed it deploys cleanly. Passed.
5. Completed/merged PR #1 into `main`.
6. The merge auto-triggered a new run; its **Deploy** stage ran
   `az deployment group create` for real.
7. Confirmed independently via `az monitor log-analytics workspace show`:
   `nypade2-logs` exists in `nypa-de2-rg`, `provisioningState: Succeeded`,
   tagged `project=nypa-energy-pipeline`, `managedBy=azure-devops-cicd`.

Two real problems came up and got fixed along the way, not glossed over:

- **New-org compute quota**: this Azure DevOps organization has 0 free
  Microsoft-hosted CI/CD parallel jobs by default (Microsoft requires a
  verified billing method to unlock the free tier, even though usage itself
  is free — an anti-abuse measure). Rather than add billing for a learning
  project, the pipeline runs on a **self-hosted agent** instead — one free
  self-hosted parallel job is available with no billing requirement at all.
  A temporary agent was registered on a local machine, used to run both the
  Validate and Deploy stages above, then fully deregistered and deleted
  immediately afterward (`config.sh remove`, process killed, local files
  removed) — nothing was left running or registered.
- **First deploy attempt failed for a real reason**: `az deployment group
  create` failed with `MissingSubscriptionRegistration` — the subscription
  wasn't registered for `Microsoft.OperationalInsights` (the Log Analytics
  resource provider) yet. Fixed with `az provider register --namespace
  Microsoft.OperationalInsights`, then reran the Deploy stage, which
  succeeded.

Every one-time resource-access grant (the service connection, the self-hosted
agent pool) required an explicit "Permit" click in the Azure DevOps UI the
first time the pipeline touched it — a built-in safeguard, not a bug.

## Analytics chatbot

[`chatbot/`](chatbot/README.md) — a natural-language-to-SQL chatbot over the gold data model
above (`nypade2_dbx.gold.*` in Databricks Unity Catalog). Modeled after the governed enterprise
pattern in [ankur715/Web_App](https://github.com/ankur715/Web_App)'s Retail Sales Analytics
Chatbot, but with the LLM's role deliberately narrowed to *interpreting the question and
drafting SQL* — authentication, row-level authorization (governmental vs. business customers),
SQL validation (parse, allow-list tables/columns, inject row filters, cap result size), and
execution all happen in code the LLM never sees or controls. FastAPI backend + single-page
chat frontend; see its README for local and Azure Container Apps setup.

## Repo layout

| Path | Contents |
|---|---|
| `infra/` | Bicep templates — resource group, ADLS Gen2, ADF, Azure SQL DB, Databricks, Key Vault, UC access connector |
| `adf/` | Exported ADF linked service / dataset / pipeline JSON (placeholders for real resource names — see `adf/README.md`) |
| `workspace/` | Databricks notebooks (source format) |
| `chatbot/` | NL-to-SQL analytics chatbot (FastAPI backend + frontend) over the gold tables — see `chatbot/README.md` |
| `scripts/` | Local helper scripts — pull NYPA sources, seed the SQL DB, init the watermark table, one-time Unity Catalog setup |
| `data/` | Small sample extracts for local testing (full pulls are gitignored) |

## Stack

Azure Data Factory · ADLS Gen2 · Azure SQL Database · Azure Key Vault · Azure Databricks
(serverless + Unity Catalog) · Delta Lake · PySpark · Bicep · FastAPI · Google Gemini

## Status

Fully working end-to-end: all 3 bronze pipelines run successfully against live NYPA
data, and the full silver → gold Databricks job completes with all 7 tasks
succeeding, producing 5 gold Delta tables. See `scripts/` for local
source-connectivity, SQL seeding, and Unity Catalog setup scripts.
