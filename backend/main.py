from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Dict

from dotenv import dotenv_values
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse


BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"


@lru_cache(maxsize=1)
def load_config() -> Dict[str, Path]:
    values = dotenv_values(ENV_PATH)
    excel_raw = (values.get("EXCEL_FILE_PATH") or "").strip()
    if not excel_raw:
        raise RuntimeError("EXCEL_FILE_PATH is missing in backend/.env")
    excel_path = Path(excel_raw).expanduser().resolve()
    return {"excel_path": excel_path}


app = FastAPI(title="Products Search Term API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/api/health")
def healthcheck() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/api/workbook")
def get_workbook() -> FileResponse:
    try:
        config = load_config()
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    workbook_path = config["excel_path"]
    if not workbook_path.exists() or not workbook_path.is_file():
        raise HTTPException(
            status_code=404,
            detail=f"Excel file not found at {workbook_path}",
        )

    return FileResponse(
        workbook_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=workbook_path.name,
    )
