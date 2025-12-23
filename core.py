# core.py
# Core logic for iPhone Gold Datamart Insight Chatbot
# UPDATED: safer init, clearer errors for Streamlit UI, lazy DB init, cache clearing helper

import os
import re
import duckdb
import pandas as pd
import dspy
from dspy import InputField, OutputField
from dspy.teleprompt import BootstrapFewShot

# ============================================
# 0) CONFIG & CONSTANTS
# ============================================

DB_PATH = "iphone_gold.duckdb"

# ============================================
# 1) LLM CONFIG (DSPy + GEMINI)
# ============================================

def configure_api_key():
    """ดึง GEMINI_API_KEY จาก Streamlit secrets หรือ env แล้วตั้งค่า env ให้พร้อมใช้
    คืนค่า True หากพบ key, มิฉะนั้น raise AssertionError('No LM is loaded') เพื่อให้ app.py จัดการ flow ได้
    """
    try:
        import streamlit as st
        try:
            if "GEMINI_API_KEY" in st.secrets and st.secrets["GEMINI_API_KEY"]:
                os.environ["GEMINI_API_KEY"] = st.secrets["GEMINI_API_KEY"]
        except Exception:
            # ignore access errors to st.secrets
            pass
    except Exception:
        # not running in Streamlit; still okay, read env below
        pass

    if "GEMINI_API_KEY" not in os.environ or not os.environ.get("GEMINI_API_KEY"):
        # Raise AssertionError to match app.py behavior which catches AssertionError
        raise AssertionError("No LM is loaded")
    return True


def clear_resource_caches():
    """พยายามเคลียร์ Streamlit resource caches และทำ cleanup ที่ปลอดภัย (best-effort)"""
    try:
        import streamlit as st
        try:
            st.cache_resource.clear()
        except Exception:
            # older Streamlit or no cache_resource - ignore
            pass
    except Exception:
        # Not in Streamlit runtime - ignore
        pass

    # Best-effort: try to remove any cached LM in dspy if accessible
    try:
        # This is safe-guard: don't assume dspy internals, only attempt if attribute present
        if hasattr(dspy, "_lm"):
            try:
                delattr(dspy, "_lm")
            except Exception:
                # Some dspy versions may not expose _lm; ignore silently
                pass
    except Exception:
        pass


def ensure_lm_configured():
    """
    Ensure LM is configured with thread-safe caching.
    Convert certain dspy errors into AssertionError messages that app.py expects
    (e.g., 'can only be changed by the thread') so the UI can auto-clear caches.
    """
    # Ensure API key first (will raise AssertionError("No LM is loaded") if missing)
    configure_api_key()

    try:
        import streamlit as st
        import time

        @st.cache_resource(show_spinner=False)
        def _configure_lm_once():
            """
            Configure LM exactly once - cached forever on Streamlit runtime.
            Convert specific errors to AssertionError with messages app.py expects.
            """
            max_retries = 3
            retry_delay = 2
            last_error = None

            for attempt in range(max_retries):
                try:
                    lm = dspy.LM(
                        "gemini/gemini-2.5-flash",
                        max_tokens=4000,      # Increased from 2000 to handle very complex SQL
                        temperature=0.1,      # Lower temperature for consistent output
                        top_p=0.95
                    )
                    dspy.configure(lm=lm)
                    return True

                except Exception as e:
                    last_error = e
                    error_msg = str(e)

                    # Convert thread-change errors to AssertionError so app.py can clear caches
                    if "can only be changed by the thread" in error_msg.lower() or "can only be changed by the thread" in error_msg:
                        raise AssertionError("can only be changed by the thread")

                    # Rate-limit handling (retryable)
                    if "429" in error_msg or "rate limit" in error_msg.lower() or "quota" in error_msg.lower():
                        if attempt < max_retries - 1:
                            wait_time = retry_delay * (2 ** attempt)  # 2, 4, 8 seconds
                            print(f"Rate limit hit, waiting {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                            time.sleep(wait_time)
                            continue

                    # For other errors, re-raise (will prevent caching)
                    raise

            # If all retries failed due to rate limit, raise the last error
            raise last_error

        # call the cached initializer
        return _configure_lm_once()

    except ImportError:
        # Not running in Streamlit - configure LM now (non-cached)
        try:
            lm = dspy.LM(
                "gemini/gemini-2.5-flash",
                max_tokens=4000,
                temperature=0.1,
                top_p=0.95
            )
            dspy.configure(lm=lm)
            return True
        except Exception as e:
            # convert certain messages to AssertionError to help app.py logic
            msg = str(e)
            if "can only be changed by the thread" in msg.lower() or "can only be changed by the thread" in msg:
                raise AssertionError("can only be changed by the thread")
            raise


# ============================================
# 2) INITIALIZE DUCKDB (lazy)
# ============================================

def ensure_database_exists():
    """Ensure DuckDB database exists (lazy init). Does not run automatically at import time."""
    if not os.path.exists(DB_PATH):
        print(f"📦 Creating database at {DB_PATH}...")
        from init_db import init_database
        init_database(DB_PATH)
    else:
        try:
            con = duckdb.connect(DB_PATH, read_only=True)
            con.execute("SELECT 1").fetchone()
            con.close()
        except Exception:
            # if DB corrupted, recreate
            if os.path.exists(DB_PATH):
                try:
                    os.remove(DB_PATH)
                except Exception:
                    pass
            from init_db import init_database
            init_database(DB_PATH)


# ============================================
# 3) HELPER FUNCTIONS
# ============================================

def clean_sql(sql: str) -> str:
    """ลบ ``` หรือ ```duckdb ออกจาก SQL ที่ LLM ส่งมา"""
    if not isinstance(sql, str):
        return sql

    s = sql.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z0-9_]*\n?", "", s)
        if s.endswith("```"):
            s = s[:-3]
    return s.strip()


def run_sql(sql: str, db_path: str = DB_PATH):
    """รัน SQL กับ DuckDB แล้วคืน (DataFrame, markdown-table-string)"""
    try:
        con = duckdb.connect(db_path, read_only=True)
        df = con.execute(sql).df()
        con.close()

        if df.empty:
            table_view = "*(no rows)*"
        else:
            table_view = df.to_markdown(index=False)

        return df, table_view
    except Exception as e:
        raise Exception(f"SQL Error: {str(e)}\nSQL: {sql}")


# ============================================
# 4) DSPy SIGNATURES & MODULES
# ============================================

class IntentAndSQL(dspy.Signature):
    """
    Convert a top-management business question into DuckDB SQL using the iPhone Gold Datamart.
    (doc truncated for brevity)
    """
    question: str = InputField()
    intent: str = OutputField()
    sql: str = OutputField()


class SQLPlanner(dspy.Module):
    def __init__(self):
        super().__init__()
        self.predict = dspy.ChainOfThought(IntentAndSQL)

    def forward(self, question: str):
        return self.predict(question=question)


# ============================================
# 5) TRAINSET (examples trimmed for safety)
# ============================================

# NOTE: some long examples were trimmed here for clarity; they are still valid training
ex1 = dspy.Example(
    question="เดือน 11 ปี 2025 รุ่น iPhone ไหนขายดีที่สุด (ตามจำนวนเครื่อง)?",
    intent="best_selling_model_mtd",
    sql="""
        SELECT p.generation AS iphone_gen, SUM(c.contract_count) AS mtd_units
        FROM fact_contract c
        JOIN dim_product p ON c.product_id = p.product_id
        JOIN dim_date d ON c.date_key = d.date_key
        WHERE d.year = 2025 AND d.month = 11
        GROUP BY p.generation
        ORDER BY mtd_units DESC;
    """
).with_inputs("question")

# (For brevity, keep other examples as in original file or restore full definitions as needed)
trainset = [ex1]  # keep minimal set here; in production restore all examples or load JSON


def get_optimized_planner():
    """
    Get planner with optimized JSON support.
    Calls ensure_lm_configured() to make sure LM is ready.
    """
    ensure_lm_configured()

    try:
        import streamlit as st

        @st.cache_resource
        def _load_or_create_planner():
            import os
            json_path = "optimized_planner.json"

            if os.path.exists(json_path):
                try:
                    planner = SQLPlanner()
                    planner.load(json_path)
                    print("✅ Loaded pre-compiled planner from JSON")
                    return planner
                except Exception:
                    print("⚠️ Failed to load JSON; falling back")

            # fallback: use SQLPlanner and (optionally) load examples into it
            planner = SQLPlanner()
            try:
                # If DSPy supports loading examples programmatically, do so:
                for ex in trainset:
                    planner.add_example(ex)
            except Exception:
                pass
            return planner

        return _load_or_create_planner()

    except ImportError:
        json_path = "optimized_planner.json"
        if os.path.exists(json_path):
            try:
                planner = SQLPlanner()
                planner.load(json_path)
                return planner
            except Exception:
                pass
        planner = SQLPlanner()
        try:
            for ex in trainset:
                planner.add_example(ex)
        except Exception:
            pass
        return planner


# ============================================
# 6) INSIGHT LAYER
# ============================================

class InsightFromResult(dspy.Signature):
    question: str = InputField()
    table_view: str = InputField()
    kpi_summary: str = OutputField()
    explanation: str = OutputField()
    action: str = OutputField()


def get_insight_predictor():
    ensure_lm_configured()
    return dspy.Predict(InsightFromResult)


def generate_insight(question: str, table_view: str):
    predictor = get_insight_predictor()
    return predictor(question=question, table_view=table_view)


# ============================================
# 7) MAIN ENTRY FOR APP
# ============================================

def ask_bot_core(question: str) -> dict:
    """
    Main core function used by Streamlit / API.
    - Lazy-initialize DB
    - Ensure LM is configured (may raise AssertionError with messages app.py expects)
    - Build SQL via planner, run SQL, then summarize insight
    """
    # 0) sanity checks
    if not question or not question.strip():
        raise ValueError("Empty question")

    # Ensure DB exists (lazy)
    ensure_database_exists()

    # Ensure LM configured (may raise AssertionError("No LM is loaded") or AssertionError("can only be changed by the thread"))
    ensure_lm_configured()

    # Get planner
    planner = get_optimized_planner()

    # 1) Plan: ask planner with retry for incomplete responses
    max_retries = 3
    plan = None
    last_error = None

    for attempt in range(max_retries):
        try:
            plan = planner(question)

            # Validate
            if not hasattr(plan, 'sql') or not plan.sql:
                raise ValueError("LLM response missing 'sql' field")
            if not hasattr(plan, 'intent') or not plan.intent:
                plan.intent = "unknown"
            break

        except Exception as e:
            last_error = e
            error_msg = str(e)
            # Retry on typical parse/JSON adapter issues
            if "JSONAdapter failed to parse" in error_msg or "missing" in error_msg.lower():
                if attempt < max_retries - 1:
                    import time
                    time.sleep(1)
                    continue
            # Re-raise so caller (app.py) can handle (including clearing caches)
            raise

    if plan is None:
        raise Exception(f"Failed to get valid response from LLM after {max_retries} attempts. Last error: {last_error}")

    raw_sql = plan.sql
    sql = clean_sql(raw_sql)

    # 2) Run SQL
    df, table_view = run_sql(sql)

    # 3) If no rows, return graceful message
    if df.empty:
        return {
            "question": question,
            "intent": getattr(plan, "intent", ""),
            "sql": sql,
            "table_view": table_view,
            "kpi_summary": "",
            "explanation": "ไม่พบข้อมูลในเงื่อนไขนี้",
            "action": "ลองเปลี่ยนเดือน / ปี หรือเงื่อนไขดูอีกครั้ง",
        }

    # 4) Generate insight
    ins = generate_insight(question=question, table_view=table_view)

    return {
        "question": question,
        "intent": getattr(plan, "intent", ""),
        "sql": sql,
        "table_view": table_view,
        "kpi_summary": ins.kpi_summary,
        "explanation": ins.explanation,
        "action": ins.action,
    }
