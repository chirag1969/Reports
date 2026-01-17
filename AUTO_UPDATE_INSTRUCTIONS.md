# Automatic GitHub Pages Data Pipeline

This guide keeps your GitHub Pages dashboard updated automatically whenever the
local Excel files change. It generates the JSON/CSV files that the dashboard
already expects (no UI changes) and pushes them to GitHub.

## 1) Install dependencies

From the repo root:

```bash
python -m pip install --upgrade pip
pip install openpyxl watchdog
```

## 2) Configure the Excel paths

Copy the example config and edit it to point to your Excel files:

```bash
cp tools/local_pipeline_config.example.json tools/local_pipeline_config.json
```

Open `tools/local_pipeline_config.json` and update the `excel_path` for each
workbook. **Do not change** the `output_path` values; they must stay:

- `preprocessed/Products Search Term 2025.json`
- `preprocessed/Products Search Term 2026.json`
- `preprocessed/Products Campaign.json`

Make sure the sheet names match exactly for each workbook (e.g., `"Sheet1"`).

## 3) Run once to verify

```bash
python auto_update_dashboard.py --once
```

If everything is configured, the script will generate the JSON/CSV files and
commit + push them to GitHub automatically.

## 4) Run continuously (foreground)

```bash
python auto_update_dashboard.py
```

Leave this running; it watches your Excel files and auto-publishes changes.

## 5) Run automatically in the background (Windows)

### Option A: Task Scheduler (recommended)

1. Open **Task Scheduler** → **Create Task**.
2. **General** tab:
   - Name: `Auto Update Dashboard`
   - Check **Run whether user is logged on or not**
3. **Triggers** tab → **New...**
   - Begin the task: **At log on**
   - (Optional) Add another trigger: **On startup**
4. **Actions** tab → **New...**
   - Action: **Start a program**
   - Program/script: `C:\Path\To\pythonw.exe`
   - Add arguments: `auto_update_dashboard.py`
   - Start in: `C:\Path\To\ADVT-US`
5. **Conditions** tab: uncheck “Start the task only if the computer is on AC power” if needed.
6. Click **OK**, then enter your Windows password to save.

### Option B: Startup shortcut

1. Press `Win + R`, enter `shell:startup`, press **Enter**.
2. Create a shortcut with target:
   ```
   C:\Path\To\pythonw.exe C:\Path\To\ADVT-US\auto_update_dashboard.py
   ```

## 6) Notes / Troubleshooting

- **Git authentication**: ensure `git push` works without prompts (use a PAT or
  SSH key).
- **Path changes**: update `tools/local_pipeline_config.json` whenever the Excel
  file path changes. The watcher notices config updates automatically.
- If you see “No usable sheet found”, double-check the sheet name and ensure the
  header row exists at the configured `header_row_index`.
