# Reports Dataset Preprocessing

This project now includes a helper script to pre-process the large Excel
workbooks into a compact JSON structure. Loading the JSON allows the browser to
skip the expensive XLSX parsing step and drastically shortens the time it takes
to render dashboards.

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
