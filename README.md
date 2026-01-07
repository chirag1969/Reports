# Reports Dataset Preprocessing

This project includes helper scripts to pre-process the large Excel workbooks
into compact JSON structures. Loading the JSON allows the browser to skip the
expensive XLSX parsing step and drastically shortens the time it takes to render
dashboards.

## Generating preprocessed data

1. Install the Python dependencies:
   ```bash
   pip install pandas numpy openpyxl
   ```
2. Run the preprocessing script against your workbook:
   ```bash
   python tools/preprocess_workbook.py "Products Campaign.xlsx"
   ```
   The command creates `preprocessed/Products Campaign.json` by default.
3. Copy the generated JSON next to `index.html` (or serve it from the same
   directory). On the next load the UI will automatically pick up the JSON file
   before falling back to the XLSX workbook.

You can pass a custom output path or select a specific sheet using the
`--output` and `--sheet` options respectively. Run the script with
`--help` to see all available flags.

## Local pipeline for GitHub Pages (recommended)

Use `tools/local_pipeline.py` to read the Excel files from your PC, export the
exact JSON files used by the dashboard, and automatically commit + push them to
GitHub. The dashboard will continue to read the same files at the same paths
(`preprocessed/Products Search Term.json` and
`preprocessed/Products Campaign.json`).

1. Install dependencies:
   ```bash
   pip install openpyxl
   ```
2. Copy the example config and update the Excel paths + sheet names:
   ```bash
   cp tools/local_pipeline_config.example.json tools/local_pipeline_config.json
   ```
3. Edit `tools/local_pipeline_config.json` so each workbook entry points to the
   correct Excel path on your PC and the exact worksheet name.
4. Run the pipeline:
   ```bash
   python tools/local_pipeline.py
   ```

The script streams large Excel files safely with `openpyxl` (read-only mode),
writes the preprocessed JSON into `preprocessed/`, then runs `git add`,
`git commit`, and `git push` so GitHub Pages can pick up the updated data.
If you also set `csv_output_path` for a workbook entry, the pipeline writes a
CSV alongside the JSON and includes it in the git commit.

Optional flags:
- `--only products-search-term` (process just one dataset)
- `--skip-push` (commit without pushing)
- `--skip-git` (generate files without git commands)
- `--commit-message "Update data"` (override the commit message)

## Partitioned export for GitHub Pages

When the workbook contains tens of thousands of rows you can generate
filter-aware data slices that the static site can load on demand:

```bash
python tools/preprocess_dashboard.py "Products Campaign.xlsx" \
  --out data --filters date store category targetingType asin \
  --partition date:month store
```

This creates a ``data/`` folder with ``index.json`` (metadata, filter values,
and partition index) plus one JSON/CSV file for each partition. The metadata is
consumed by the front-end helper in ``tools/frontend-data-loader.js`` to fetch
only the slices required by the active filters and to keep every pivot table in
sync.
