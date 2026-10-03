# Dashboard in Cloudera Data Visualization

One dashboard, built as code and imported into the federal CDW Data Visualization instance
(`CASA_CDV_URL`, shared with other projects): **rsingh-casa-alb - CASA ALCO**, the month-end
ALCO pack (core split, RBI SLS slotting, TimesFM forecast, run history and trust numbers). It
sits in this project's own workspace `rsingh-casa-alb`. Nothing in the pipeline reads it; it
only reads.

| Layer | What | Where |
|---|---|---|
| Views | 5 flat reporting views, one per dataset; `is_latest = 1` marks the latest ALCO run | `sql/dataviz_views.sql` -> `rsingh_casa_alb_gold` |
| Connection | `rsingh-casa-alb-impala`: impyla, CDW `federal-impala-1` public endpoint, port 443, HTTP `cliservice`, TLS, LDAP as the workload user | created by `dataviz/build_dashboard.py` |
| Workspace | `rsingh-casa-alb` | created by `dataviz/build_dashboard.py` |
| Datasets, visuals, dashboard | 5 datasets (`rsingh-casa-alb - ...`), 14 visuals, 1 dashboard of 4 sheets, fixed UUIDs | `dataviz/build_dashboard.py` -> `dataviz/casa_alco_dashboard.json` |

## Sheets

- **ALCO overview** (latest run): total CASA, core, core share, segments where the BCBS cap
  binds; core and non-core by IRRBB category; model core share against the cap by segment;
  the core split table per segment.
- **Structural liquidity**: RBI SLS buckets by component and by IRRBB category (latest run).
- **Forecast**: the 52-week P10 / P50 / P90 band of total CASA (latest run); weekly CASA
  balance by IRRBB category over the whole history.
- **History and trust**: core share by ALCO run and segment; core and non-core by run; every
  ALCO run with its backtest numbers, model revision, Iceberg snapshot and `triggered_by`.

## Build or rebuild

```bash
set -a; source .env; set +a
python dataviz/build_dashboard.py              # views, connection, workspace, file, import
python dataviz/build_dashboard.py --no-import  # views and the export file only
python dataviz/build_dashboard.py --verify     # every visual's query through the Data API
```

`python ci/setup_cai.py --dataviz` runs the same build. The script authenticates with the
instance's API key (`CASA_CDV_API_KEY`, or `CASA_VIZ_API_KEY`) as `Authorization: apikey ...`
(a Bearer header gets 401). The import matches artefacts by UUID, so a rerun updates the
dashboard in place. The import ignores the workspace inside the file and puts the dashboard in
the importer's Private workspace unless the form field `workspace=<name>` is sent (a workspace
id is rejected); the script sends the name. `--verify` sends every visual's query through the
Data API, so it checks Data Visualization's own connection to Impala, not the laptop's.

## Notes

- The instance is shared: the script only creates or updates the connection, workspace,
  datasets and dashboard named in `dataviz/build_dashboard.py`.
- The connection stores the workload password inside Data Visualization; rotate it there
  (Data -> connection -> Edit) when the password changes.
- The dashboard always shows the latest run, so the monthly DAG refreshes it without a
  rebuild; re-run the build only when the views or visuals change.
