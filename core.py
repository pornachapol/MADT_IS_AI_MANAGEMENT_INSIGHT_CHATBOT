# core.py
# Core logic for iPhone Gold Datamart Insight Chatbot
# FULL VERSION (9 Examples + Auto-Discovery Model)

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
# 1) LLM CONFIG (AUTO-DISCOVERY MODE)
# ============================================

def load_lm():
    """
    Load LM with Auto-Failover.
    ระบบจะลองเชื่อมต่อ Model ทีละตัว จนกว่าจะเจอตัวที่ใช้งานได้ (แก้ปัญหา 404)
    """
    # 1. Setup Keys
    try:
        import streamlit as st
        if "GEMINI_API_KEY" in st.secrets:
            api_key = st.secrets["GEMINI_API_KEY"]
            os.environ["GEMINI_API_KEY"] = api_key
            os.environ["GOOGLE_API_KEY"] = api_key
    except Exception:
        pass

    if "GOOGLE_API_KEY" not in os.environ and "GEMINI_API_KEY" not in os.environ:
         raise ValueError("ไม่พบ API Key! กรุณาตรวจสอบ .streamlit/secrets.toml")

    # 2. 🔥 รายชื่อ Model ที่จะไล่ลอง (กันเหนียว 404)
    candidate_models = [
        "gemini/gemini-1.5-flash-latest", # ลองตัวล่าสุด
        "gemini/gemini-1.5-flash-001",    # ลองตัว version เจาะจง
        "gemini/gemini-1.5-flash",        # ลองตัว alias ปกติ
        "gemini/gemini-1.5-pro-latest",   # ลองตัว Pro
        "gemini/gemini-pro"               # ไม้ตายสุดท้าย
    ]

    lm = None
    print("🔄 Connecting to Gemini (Auto-Discovery Mode)...")

    for model in candidate_models:
        try:
            print(f"   👉 Testing: {model} ...")
            # ลองสร้าง Object
            temp_lm = dspy.LM(model)
            # ลองยิง Test Request 1 ครั้ง
            dspy.configure(lm=temp_lm)
            dspy.Predict("test_in -> test_out")(test_in="ping")
            
            # ถ้าผ่าน แสดงว่าใช้ได้
            print(f"   ✅ CONNECTED! Using: {model}")
            lm = temp_lm
            break
        except Exception as e:
            print(f"   ❌ Failed ({model}): {str(e)}")
            continue

    if lm is None:
        # ถ้าหาไม่เจอเลย ให้ใช้ท่าไม้ตายสุดท้าย (Native Client)
        print("⚠️ All litellm attempts failed. Switching to Native Google Client...")
        lm = dspy.Google(model="gemini-1.5-flash", api_key=os.environ["GOOGLE_API_KEY"])

    dspy.configure(lm=lm)
    return lm

# Load Global LM
GLOBAL_LM = load_lm()


# Initialize database
def ensure_database_exists():
    """Ensure DuckDB database exists, create from CSV if needed"""
    if not os.path.exists(DB_PATH):
        print(f"📦 Creating database at {DB_PATH}...")
        try:
            from init_db import init_database
            init_database(DB_PATH)
        except ImportError:
            print("⚠️ Error: init_db.py not found.")
    else:
        try:
            con = duckdb.connect(DB_PATH, read_only=True)
            con.execute("SELECT 1").fetchone()
            con.close()
        except Exception:
            if os.path.exists(DB_PATH): os.remove(DB_PATH)
            from init_db import init_database
            init_database(DB_PATH)

ensure_database_exists()


# ============================================
# 2) HELPER FUNCTIONS
# ============================================

def clean_sql(sql: str) -> str:
    """
    ลบ code fence พวก ``` หรือ ```duckdb ออกจาก SQL ที่ LLM ส่งมา
    """
    if not isinstance(sql, str): return sql
    s = sql.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z0-9_]*\n?", "", s)
        if s.endswith("```"): s = s[:-3]
    return s.strip()

def run_sql(sql: str, db_path: str = DB_PATH):
    """
    รัน SQL กับ DuckDB แล้วคืน (DataFrame, markdown-table-string)
    """
    try:
        con = duckdb.connect(db_path, read_only=True)
        df = con.execute(sql).df()
        con.close()
        return df, (df.to_markdown(index=False) if not df.empty else "*(no rows)*")
    except Exception as e:
        raise Exception(f"SQL Error: {str(e)}\nSQL: {sql}")


# ============================================
# 3) DSPy SIGNATURES & MODULES
# ============================================

class IntentAndSQL(dspy.Signature):
    """
    Convert a top-management business question into DuckDB SQL using the iPhone Gold Datamart.

    YOU MUST OBEY THESE RULES STRICTLY:

    1) Allowed tables and columns (use ONLY these, nothing else):
       fact_registration(date_key, branch_id, product_id, reg_count)
       fact_contract(date_key, branch_id, product_id, contract_count)
       fact_inventory_snapshot(date_key, branch_id, product_id, stock_qty)
       dim_date(date_key, date, year, month, day)
       dim_product(product_id, model_name, generation, storage_gb, color, list_price)
       dim_branch(branch_id, branch_code, branch_name, region)

    2) You MUST NOT invent any new tables or columns.
    3) Joins must use correct keys.
    4) Dates: date_key is INT in YYYYMMDD.
    5) Revenue vs Units:
       - Revenue: SUM(contract_count * list_price)
       - Units: SUM(contract_count)
    """
    question: str = InputField(desc="Top-management analytics question")
    intent: str = OutputField(desc="Short intent name")
    sql: str = OutputField(desc="Valid DuckDB SQL")
    comment: str = OutputField(desc="Short explanation")

class SQLPlanner(dspy.Module):
    def __init__(self):
        super().__init__()
        self.predict = dspy.ChainOfThought(IntentAndSQL)
    def forward(self, question: str):
        return self.predict(question=question)


# ============================================
# 4) TRAINSET (FULL 9 EXAMPLES - NO CUTS)
# ============================================

# 1) Best-selling iPhone generation in Nov (MTD)
ex1 = dspy.Example(
    question="เดือน 11 ปี 2025 รุ่น iPhone ไหนขายดีที่สุด (ตามจำนวนเครื่อง)?",
    intent="best_selling_model_mtd",
    sql="""
        SELECT
            p.generation AS iphone_gen,
            SUM(c.contract_count) AS mtd_units
        FROM fact_contract c
        JOIN dim_product p ON c.product_id = p.product_id
        JOIN dim_date d    ON c.date_key   = d.date_key
        WHERE d.year = 2025
          AND d.month = 11
        GROUP BY p.generation
        ORDER BY mtd_units DESC;
    """,
    comment="Calculate total units sold per iPhone generation in Nov 2025 and rank from highest to lowest."
).with_inputs("question")

# 2) Best branch by units sold in Nov
ex2 = dspy.Example(
    question="ในเดือนพฤศจิกายน 2025 สาขาไหนมียอดขายเครื่องมากที่สุด?",
    intent="best_branch_mtd",
    sql="""
        SELECT
            b.branch_code,
            b.branch_name,
            SUM(c.contract_count) AS total_units_sold
        FROM fact_contract c
        JOIN dim_branch b ON c.branch_id = b.branch_id
        JOIN dim_date d  ON c.date_key   = d.date_key
        WHERE d.year = 2025
          AND d.month = 11
        GROUP BY b.branch_code, b.branch_name
        ORDER BY total_units_sold DESC;
    """,
    comment="Sum contract_count by branch for Nov 2025 and sort branches by total units sold."
).with_inputs("question")

# 3) Conversion rate per branch (Reg → Contract, Nov 2025)
ex3 = dspy.Example(
    question="ช่วยดู Conversion Rate ของแต่ละสาขาในเดือน 11 ปี 2025 ให้หน่อย",
    intent="branch_conversion_mtd",
    sql="""
        SELECT
            b.branch_code,
            b.branch_name,
            SUM(r.reg_count) AS total_reg,
            SUM(COALESCE(c.contract_count, 0)) AS total_contract,
            CASE
                WHEN SUM(r.reg_count) = 0 THEN NULL
                ELSE ROUND(SUM(COALESCE(c.contract_count, 0)) * 1.0 / SUM(r.reg_count), 2)
            END AS conversion_rate
        FROM fact_registration r
        JOIN dim_branch b ON r.branch_id = b.branch_id
        JOIN dim_date d   ON r.date_key   = d.date_key
        LEFT JOIN fact_contract c
          ON r.date_key   = c.date_key
          AND r.branch_id  = c.branch_id
          AND r.product_id = c.product_id
        WHERE d.year = 2025
          AND d.month = 11
        GROUP BY b.branch_code, b.branch_name
        ORDER BY conversion_rate DESC NULLS LAST;
    """,
    comment="Compare total registrations vs contracts per branch in Nov 2025 to compute conversion rate."
).with_inputs("question")

# 4) Lost opportunity by branch on 2025-11-11
ex4 = dspy.Example(
    question="วันที่ 11/11/2025 สาขาไหนเสียโอกาสขาย (Demand > Stock) สูงที่สุด?",
    intent="lost_opportunity_by_branch_on_date",
    sql="""
        SELECT
            b.branch_code,
            b.branch_name,
            SUM(r.reg_count) AS demand,
            SUM(i.stock_qty) AS stock,
            SUM(r.reg_count) - SUM(i.stock_qty) AS lost_opportunity
        FROM fact_registration r
        JOIN fact_inventory_snapshot i
          ON r.date_key   = i.date_key
          AND r.branch_id  = i.branch_id
          AND r.product_id = i.product_id
        JOIN dim_branch b ON r.branch_id = b.branch_id
        WHERE r.date_key = 20251111
        GROUP BY b.branch_code, b.branch_name
        HAVING SUM(r.reg_count) > SUM(i.stock_qty)
        ORDER BY lost_opportunity DESC;
    """,
    comment="For 2025-11-11, find branches where demand exceeds stock and rank by lost opportunity."
).with_inputs("question")

# 5) Hot SKU with low stock on 2025-11-11
ex5 = dspy.Example(
    question="วันที่ 11/11/2025 มี SKU ไหนที่ขายดีแต่สต็อกคงเหลือน้อยกว่าเท่ากับ 5 เครื่องบ้าง?",
    intent="hot_sku_low_stock_on_date",
    sql="""
        SELECT
            b.branch_code,
            b.branch_name,
            p.product_id,
            p.model_name,
            p.generation,
            p.storage_gb,
            p.color,
            SUM(c.contract_count) AS units_sold_today,
            SUM(i.stock_qty)      AS stock_remaining
        FROM fact_contract c
        JOIN fact_inventory_snapshot i
          ON c.date_key   = i.date_key
          AND c.branch_id  = i.branch_id
          AND c.product_id = i.product_id
        JOIN dim_branch b  ON c.branch_id  = b.branch_id
        JOIN dim_product p ON c.product_id = p.product_id
        WHERE c.date_key = 20251111
        GROUP BY
            b.branch_code,
            b.branch_name,
            p.product_id,
            p.model_name,
            p.generation,
            p.storage_gb,
            p.color
        HAVING SUM(c.contract_count) > 0
           AND SUM(i.stock_qty) <= 5
        ORDER BY stock_remaining ASC, units_sold_today DESC;
    """,
    comment="On 2025-11-11, identify SKUs that sold at least 1 unit and have remaining stock ≤ 5."
).with_inputs("question")

# 6) Daily sales trend for Nov 2025
ex6 = dspy.Example(
    question="ขอดูยอดขายต่อวันในเดือนพฤศจิกายน 2025 รวมทุกสาขาให้หน่อย",
    intent="daily_sales_trend_mtd",
    sql="""
        SELECT
            d.date,
            SUM(c.contract_count) AS total_units_sold
        FROM fact_contract c
        JOIN dim_date d ON c.date_key = d.date_key
        WHERE d.year = 2025
          AND d.month = 11
        GROUP BY d.date
        ORDER BY d.date;
    """,
    comment="Show total units sold per day in November 2025 across all branches."
).with_inputs("question")

# 7) Demand by generation (registration) in Nov 2025
ex7 = dspy.Example(
    question="เดือน 11 ปี 2025 ลูกค้าสนใจ iPhone แต่ละรุ่น (จาก Registration) เท่าไหร่?",
    intent="demand_by_generation_mtd",
    sql="""
        SELECT
            p.generation AS iphone_gen,
            SUM(r.reg_count) AS total_reg
        FROM fact_registration r
        JOIN dim_product p ON r.product_id = p.product_id
        JOIN dim_date d   ON r.date_key   = d.date_key
        WHERE d.year = 2025
          AND d.month = 11
        GROUP BY p.generation
        ORDER BY total_reg DESC;
    """,
    comment="Count total registrations by iPhone generation for November 2025."
).with_inputs("question")

# 8) Stock depth for iPhone 17 256GB on 2025-11-11
ex8 = dspy.Example(
    question="วันที่ 11/11/2025 สาขาไหนมีสต็อก iPhone 17 256GB รวมกันมากที่สุด?",
    intent="stock_depth_specific_model_on_date",
    sql="""
        SELECT
            b.branch_code,
            b.branch_name,
            SUM(i.stock_qty) AS total_stock
        FROM fact_inventory_snapshot i
        JOIN dim_branch b  ON i.branch_id  = b.branch_id
        JOIN dim_product p ON i.product_id = p.product_id
        WHERE i.date_key = 20251111
          AND p.generation = '17'
          AND p.storage_gb = 256
        GROUP BY b.branch_code, b.branch_name
        ORDER BY total_stock DESC;
    """,
    comment="For 2025-11-11, compute stock quantity of iPhone 17 256GB per branch and rank by stock depth."
).with_inputs("question")

# 9) Monthly revenue vs previous month (money)
ex9 = dspy.Example(
    question="เดือน 11 ปี 2025 เทียบกับเดือน 10 ปี 2025 ยอดขายเป็นเงินรวมเป็นยังไง?",
    intent="monthly_revenue_vs_prev_month",
    sql="""
        WITH monthly_revenue AS (
            SELECT
                d.year,
                d.month,
                SUM(c.contract_count * p.list_price) AS total_revenue
            FROM fact_contract c
            JOIN dim_date d    ON c.date_key   = d.date_key
            JOIN dim_product p ON c.product_id = p.product_id
            WHERE d.year = 2025
              AND d.month IN (10, 11)
            GROUP BY d.year, d.month
        )
        SELECT
            cur.year,
            cur.month           AS current_month,
            cur.total_revenue   AS current_revenue,
            prev.month          AS prev_month,
            prev.total_revenue  AS prev_revenue,
            cur.total_revenue - prev.total_revenue AS diff_revenue,
            CASE
                WHEN prev.total_revenue = 0 THEN NULL
                ELSE ROUND(
                    (cur.total_revenue - prev.total_revenue) * 100.0 / prev.total_revenue,
                    2
                )
            END AS growth_pct
        FROM monthly_revenue cur
        LEFT JOIN monthly_revenue prev
          ON cur.year  = prev.year
          AND cur.month = 11
          AND prev.month = 10;
    """,
    comment="Compare November 2025 revenue vs October 2025 by multiplying contract_count * list_price and computing difference and growth percentage."
).with_inputs("question")

trainset = [ex1, ex2, ex3, ex4, ex5, ex6, ex7, ex8, ex9]


teleprompter = BootstrapFewShot(metric=lambda x, y, trace=None: 0.0)
optimized_planner = teleprompter.compile(SQLPlanner(), trainset=trainset)

# ============================================
# 5) INSIGHT LAYER
# ============================================

class InsightFromResult(dspy.Signature):
    """
    Turn a SQL result table into management insight and actions in Thai (B1 level).
    """
    question: str = InputField(desc="Original management question in Thai or English")
    table_view: str = InputField(desc="SQL result as a small markdown table")
    kpi_summary: str = OutputField(desc="Short bullet list of key KPIs in Thai (B1)")
    explanation: str = OutputField(desc="Insight explanation in Thai (B1)")
    action: str = OutputField(desc="1–3 recommended actions in Thai (B1)")

insight_predictor = dspy.Predict(InsightFromResult)
def generate_insight(question: str, table_view: str):
    return insight_predictor(question=question, table_view=table_view)


# ============================================
# 6) MAIN ENTRY FOR APP (CRASH PROOF)
# ============================================

def ask_bot_core(question: str) -> dict:
    # 1. ย้ำ Config (แก้ปัญหา Streamlit Thread)
    dspy.configure(lm=GLOBAL_LM)

    # 2. Plan SQL (ใส่ Try Catch ป้องกัน Error NoneType/JSON)
    try:
        plan = optimized_planner(question)
    except Exception as e:
        error_msg = str(e)
        # กรณี API ส่งค่าว่างกลับมา (ที่ทำให้เกิด JSON Error)
        return {
            "question": question, "intent": "error", "sql": "", "table_view": "",
            "kpi_summary": "Connection Error", 
            "explanation": f"ระบบ AI ขัดข้องชั่วคราว: {error_msg}",
            "action": "กรุณาลองกดถามใหม่อีกครั้ง"
        }

    raw_sql = plan.sql
    sql = clean_sql(raw_sql)

    # 3. Run SQL
    try:
        df, table_view = run_sql(sql)
    except Exception as e:
         return {
            "question": question, "intent": getattr(plan, "intent", "sql_error"), "sql": sql,
            "table_view": "Error", "kpi_summary": "SQL Error", 
            "explanation": f"SQL ผิดพลาด: {str(e)}", "action": "ลองถามใหม่ด้วยคำพูดที่ชัดเจนขึ้น"
        }

    # 4. Handle Empty
    if df.empty:
        return {
            "question": question, "intent": getattr(plan, "intent", ""), "sql": sql, "table_view": table_view,
            "kpi_summary": "", "explanation": "ไม่พบข้อมูลในเงื่อนไขนี้", "action": "ลองปรับช่วงเวลาหรือเงื่อนไข"
        }

    # 5. Insight
    try:
        ins = generate_insight(question=question, table_view=table_view)
    except Exception:
        # Fallback Insight
        class Dummy:
             kpi_summary="สรุปข้อมูลตามตาราง"
             explanation="แสดงข้อมูลตามที่ร้องขอ"
             action="-"
        ins = Dummy()

    return {
        "question": question,
        "intent": getattr(plan, "intent", ""),
        "sql": sql,
        "table_view": table_view,
        "kpi_summary": ins.kpi_summary,
        "explanation": ins.explanation,
        "action": ins.action,
    }
