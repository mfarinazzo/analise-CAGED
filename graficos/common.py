from __future__ import annotations

import json
import math
import sqlite3
from pathlib import Path
from typing import Any, Dict


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / 'projeto_caged.db'
VIZ_DIR = BASE_DIR / 'viz'
DATA_DIR = VIZ_DIR / 'data'


def ensure_dirs() -> None:
    VIZ_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def connect() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise FileNotFoundError(f'Banco não encontrado: {DB_PATH}')
    return sqlite3.connect(DB_PATH)


def save_json(name: str, payload: Dict[str, Any]) -> Path:
    ensure_dirs()
    out_path = DATA_DIR / f'{name}.json'
    # Sanitize NaN/Inf recursively to emit valid JSON (use null instead)
    def _sanitize(obj: Any) -> Any:
        try:
            import numpy as _np  # type: ignore
        except Exception:  # pragma: no cover - numpy should exist via pandas, but be defensive
            _np = None  # type: ignore

        if obj is None:
            return None
        if isinstance(obj, dict):
            return {k: _sanitize(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_sanitize(v) for v in obj]
        # Normalize numpy scalars
        if 'numpy' in str(type(obj)):
            try:
                # Convert numpy numeric types to native Python
                if _np is not None and isinstance(obj, (_np.floating, _np.integer)):
                    obj = obj.item()
            except Exception:
                pass
        if isinstance(obj, float):
            return obj if math.isfinite(obj) else None
        return obj

    cleaned = _sanitize(payload)
    with out_path.open('w', encoding='utf-8') as f:
        json.dump(cleaned, f, ensure_ascii=False, allow_nan=False)
    return out_path
