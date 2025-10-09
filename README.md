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
   python tools/preprocess_workbook.py "Products Search Term.xlsx"
   ```
   The command creates `preprocessed/Products Search Term.json` by default.
3. Copy the generated JSON next to `index.html` (or serve it from the same
   directory). On the next load the UI will automatically pick up the JSON file
   before falling back to the XLSX workbook.

You can pass a custom output path or select a specific sheet using the
`--output` and `--sheet` options respectively. Run the script with
`--help` to see all available flags.

## Partitioned export for GitHub Pages

When the workbook contains tens of thousands of rows you can generate
filter-aware data slices that the static site can load on demand:

```bash
python tools/preprocess_dashboard.py "Products Search Term.xlsx" \
  --out data --filters date store category targetingType asin \
  --partition date:month store
```

This creates a ``data/`` folder with ``index.json`` (metadata, filter values,
and partition index) plus one JSON/CSV file for each partition. The metadata is
consumed by the front-end helper in ``tools/frontend-data-loader.js`` to fetch
only the slices required by the active filters and to keep every pivot table in
sync.
