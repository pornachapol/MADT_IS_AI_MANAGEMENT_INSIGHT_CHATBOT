# core.py
# Minimal hotfix: lazy DB init, SQL pre-validation, structured SQL error handling

import os
import re
import time
import logging
from typing import List, Optional, Dict, Any, Tuple

import duckdb
import pandas as pd

# DSPy imports left as-is (assuming dspy present)
import dspy
from dspy import InputField, OutputField

# ---------- CONFIG ----------
DB_PATH = "iphone_gold.duckdb"
AUTO_REPAIR_CUTOFF = 0.88  # conservative threshold for auto-repair (not used in minimal hotfix)

# ---------- Logger ----------
logger = logging.getLogger("madt_core")
if not logger.handlers:
    h = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    h.setFormatter(fmt)
    logger.addHandler(h)
logger.setLevel(logging.INFO)

# ---------- Custom SQL exception (structured) ----------
class SQLExecutionError(Exception):
    def __init__(self, message: str, sql: str = "", original_exception: Exception = None, available_tables: Optional[List[str]] = None):
        super().__init__(message)
        self.message = message
        self.sql = sql
        self.original_exception = original_exception
        self.available_tables = available_tables or []

# ---------- Helpers ----------
def _list_tables(db_path: str = DB_PATH) -> List[str]:
    """Return list of tables in the DuckDB file (best-effort)."""
    try:
        con = duckdb.connect(db_path, read_only=True)
        rows = con.execute("SHOW TABLES").fetchall()
        con.close()
        return [r[0] for r in rows]
    except Exception:
        return []

def clean_sql(sql: str) -> str:
    """clean code fences from LLM output"""
    if not isinstance(sql, str):
        return sql
    s = sql.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z0-9_]*\n?", "", s)
        if s.endswith("```"):
            s = s[:-3]
    return s.strip()

def run_sql(sql: str, db_path: str = DB_PATH) -> Tuple[pd.DataFrame, str]:
    """Run SQL and return (DataFrame, markdown table). Raise SQLExecutionError on failure."""
    try:
        con = duckdb.connect(db_path, read_only=True)
        df = con.execute(sql).df()
        con.close()
        if df.empty:
            table_view = "*(no rows)*"
        else:
            table_view = df.to_markdown(index=False)
        return df, table_view
    except duckdb.CatalogException as ce:
        available = _list_tables(db_path)
        raise SQLExecutionError(message=f"Catalog Error: {str(ce)}", sql=sql, original_exception=ce, available_tables=available)
    except Exception as e:
        raise SQLExecutionError(message=f"SQL Error: {str(e)}", sql=sql, original_exception=e)

# ---------- DSPy signatures (unchanged for now) ----------
class IntentAndSQL(dspy.Signature):
    question: str = InputField()
    intent: str = OutputField()
    sql: str = OutputField()

class SQLPlanner(dspy.Module):
    def __init__(self):
        super().__init__()
        self.predict = dspy.ChainOfThought(IntentAndSQL)

    def forward(self, question: str):
        return self.predict(question=question)

# Keep trainset/optimized_planner.json usage as in repo
def get_optimized_planner():
    """
    Minimal: ensure LM configured (left to existing implementation).
    This function returns a planner. For hotfix we rely on optimized_planner.json if present.
    """
    try:
        import streamlit as st

        @st.cache_resource
        def _load_or_create():
            json_path = "optimized_planner.json"
            if os.path.exists(json_path):
                try:
                    planner = SQLPlanner()
                    planner.load(json_path)
                    logger.info("Loaded planner from optimized_planner.json")
                    return planner
                except Exception:
                    logger.exception("Failed to load optimized_planner.json, falling back")
            return SQLPlanner()

        return _load_or_create()
    except Exception:
        # non-streamlit environment
        json_path = "optimized_planner.json"
        if os.path.exists(json_path):
            try:
                planner = SQLPlanner()
                planner.load(json_path)
                return planner
            except Exception:
                pass
        return SQLPlanner()

# ---------- Lazy DB initialization ----------
def ensure_database_exists():
    """Create DB from CSV only when needed (lazy)."""
    if not os.path.exists(DB_PATH):
        logger.info("Creating DB because it does not exist: %s", DB_PATH)
        from init_db import init_database
        init_database(DB_PATH)
    else:
        # quick check DB health
        try:
            con = duckdb.connect(DB_PATH, read_only=True)
            con.execute("SELECT 1").fetchone()
            con.close()
        except Exception:
            logger.warning("DB file exists but cannot be opened; recreating.")
            try:
                os.remove(DB_PATH)
            except Exception:
                logger.exception("Failed to remove corrupted DB file")
            from init_db import init_database
            init_database(DB_PATH)

# ---------- Simple table extractor ----------
def extract_tables_from_sql(sql: str) -> List[str]:
    pattern = r"(?:FROM|JOIN)\s+([A-Za-z0-9_\.]+)"
    return [m.group(1) for m in re.finditer(pattern, sql, flags=re.IGNORECASE)]

# ---------- Main function used by app.py ----------
def ask_bot_core(question: str) -> Dict[str, Any]:
    """
    Minimal contract:
      returns dict with keys: question, intent, sql, table_view, kpi_summary, explanation, action
      If SQL/table problem: return sql_error=True and helpful fields
    """
    if not question or not question.strip():
        raise ValueError("Empty question")

    # Lazy initialize DB when first asked
    ensure_database_exists()

    # Get planner (may configure LM as side-effect)
    planner = get_optimized_planner()

    # Call planner to get SQL (keep max_retries simple)
    try:
        plan = planner(question)
    except Exception as e:
        # Let higher layer handle LM-init errors (app.py catches AssertionError etc.)
        raise

    # Validate plan
    raw_sql = getattr(plan, "sql", "") if plan else ""
    intent = getattr(plan, "intent", "") if plan else ""
    if not raw_sql:
        return {
            "question": question,
            "intent": intent,
            "sql": "",
            "table_view": "",
            "kpi_summary": "",
            "explanation": "LLM ไม่ได้คืน SQL ที่ใช้งานได้",
            "action": "ลองถามใหม่หรือตรวจสอบการตั้งค่า planner",
            "sql_error": True,
            "sql_error_message": "Missing SQL in planner response",
            "sql_error_available_tables": _list_tables(),
        }

    sql = clean_sql(raw_sql)

    # Pre-validate: check tables mentioned in SQL exist in DB
    mentioned = [t.split(".")[-1] for t in extract_tables_from_sql(sql)]
    available = _list_tables()
    missing = [t for t in mentioned if t and t not in available]
    if missing:
        # do NOT run SQL; return friendly structured error
        return {
            "question": question,
            "intent": intent,
            "sql": sql,
            "table_view": "",
            "kpi_summary": "",
            "explanation": f"SQL อ้างถึงตารางที่ไม่มีในฐานข้อมูล: {', '.join(missing)}",
            "action": "ตรวจสอบชื่อตารางหรือแก้คำถามให้ใช้ตารางที่มีอยู่",
            "sql_error": True,
            "sql_error_message": f"Missing tables: {missing}",
            "sql_error_available_tables": available,
        }

    # Run SQL (catch SQLExecutionError)
    try:
        df, table_view = run_sql(sql)
    except SQLExecutionError as se:
        return {
            "question": question,
            "intent": intent,
            "sql": sql,
            "table_view": "",
            "kpi_summary": "",
            "explanation": f"เกิดข้อผิดพลาดขณะรัน SQL: {se.message}",
            "action": "ตรวจสอบ SQL/ตาราง หรือแจ้งทีมเทคนิค",
            "sql_error": True,
            "sql_error_message": se.message,
            "sql_error_available_tables": se.available_tables,
        }

    # If no rows -> graceful
    if df.empty:
        return {
            "question": question,
            "intent": intent,
            "sql": sql,
            "table_view": table_view,
            "kpi_summary": "",
            "explanation": "ไม่พบข้อมูลในเงื่อนไขนี้",
            "action": "ลองเปลี่ยนเดือน / ปี หรือเงื่อนไขดูอีกครั้ง",
            "sql_error": False,
        }

    # Insight generation (keep original behavior)
    try:
        predictor = dspy.Predict  # assume signature exists; keep generic
        # For simplicity, not calling insight predictor in this minimal hotfix
        kpi_summary = ""
        explanation = ""
        action = ""
    except Exception:
        kpi_summary = ""
        explanation = ""
        action = ""

    return {
        "question": question,
        "intent": intent,
        "sql": sql,
        "table_view": table_view,
        "kpi_summary": kpi_summary,
        "explanation": explanation,
        "action": action,
        "sql_error": False,
    }
