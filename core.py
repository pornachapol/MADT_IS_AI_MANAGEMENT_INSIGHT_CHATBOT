# core.py
# Core logic for iPhone Gold Datamart Insight Chatbot

import os
import re
import duckdb
import pandas as pd
import dspy
from dspy import InputField, OutputField
from dspy.teleprompt import BootstrapFewShot

# ============================================
# 0) LLM / DSPy CONFIG
# ============================================

# GEMINI_API_KEY ควรถูกตั้งใน environment (เช่น Streamlit secrets → os.environ)
# ตัวอย่าง (ห้ามเขียน key ลงไฟล์นี้ตรง ๆ เวลาใช้จริง):
# os.environ["GEMINI_API_KEY"] = "YOUR_KEY"

dspy.settings.configure(
    lm=dspy.LM("gemini/gemini-2.5-flash")
)

DB_PATH = "iphone_gold.duckdb"


# ============================================
# 1) HELPER: CLEAN SQL + RUN SQL
# ============================================

def clean_sql(sql: str) -> str:
    """
    ลบ code fence พวก ``` หรือ ```duckdb ออกจาก SQL ที่ LLM ส่งมา
    ให้เหลือเฉพาะ SQL ล้วน ๆ
    """
    if not isinstance(sql, str):
        return sql

    s = sql.strip()

    # ถ้าเริ่มด้วย ``` (เช่น ```sql, ```duckdb)
    if s.startswith("```"):
        # ตัดบรรทัดแรก (```xxx) ออก
        s = re.sub(r"^```[a-zA-Z0-9_]*\n?", "", s)
        # ถ้าจบด้วย ``` ให้ตัด ``` ทิ้ง
        if s.endswith("```"):
            s = s[:-3]

    return s.strip()


def run_sql(sql: str, db_path: str = DB_PATH):
    """
    รัน SQL กับ DuckDB แล้วคืน (DataFrame, markdown-table-string)
    """
    con = duckdb.connect(db_path)
    df = con.execute(sql).df()
    con.close()

    if df.empty:
        table_view = "*(no rows)*"
    else:
        table_view = df.to_markdown(index=False)

    return df, table_view


# ============================================
# 2) DSPy SIGNATURE: Intent + SQL
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
       For example, NEVER use: fact_sales, dim_model, dim_store, model_key, quantity, etc.

    3) Joins:
       fact_*.date_key   = dim_date.date_key
       fact_*.branch_id  = dim_branch.branch_id
       fact_*.product_id = dim_product.product_id

    4) Dates:
       - date_key is INT in YYYYMMDD (e.g. 20251111).
       - "เดือน X ปี Y"  => filter using dim_date.year = Y AND dim_date.month = X.
       - If year is not mentioned, assume the latest year in the data.

    Your job:
       - Understand the question.
       - Propose a short intent name.
       - Generate a VALID DuckDB SQL that respects ALL rules above.
       - Briefly explain what the SQL does.
    """

    question: str = InputField(desc="Top-management analytics question in Thai or English")
    intent: str   = OutputField(desc="Short intent name for the question, e.g. best_branch_mtd")
    sql: str      = OutputField(desc="Valid DuckDB SQL over the given schema and rules")
    comment: str  = OutputField(desc="Short English explanation of what the SQL does")


class SQLPlanner(dspy.Module):
    def __init__(self):
        super().__init__()
        self.predict = dspy.ChainOfThought(IntentAndSQL)

    def forward(self, question: str):
        return self.predict(question=question)


# ============================================
# 3) TRAINSET (8 EXAMPLES) + TELEPROMPTER
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

trainset = [ex1, ex2, ex3, ex4, ex5, ex6, ex7, ex8]


def dummy_metric(example, prediction, trace=None):
    # ยังไม่ได้ใช้ metric จริง ตอนนี้แค่ให้ teleprompter ทำ few-shot rewrite
    return 0.0


teleprompter = BootstrapFewShot(metric=dummy_metric)
optimized_planner = teleprompter.compile(SQLPlanner(), trainset=trainset)


# ============================================
# 4) INSIGHT LAYER
# ============================================

class InsightFromResult(dspy.Signature):
    """
    Turn a SQL result table into management insight and actions in Thai (B1 level).

    Guideline:
    - kpi_summary: bullet สั้นๆ สรุปตัวเลขสำคัญ
    - explanation: อธิบายความหมายของตัวเลขต่อธุรกิจ (Demand–Sales–Stock)
    - action: แนะนำ 1–3 ข้อควรทำต่อ (ปรับสต็อก, โปรโมชัน, โฟกัสสาขา ฯลฯ)
    """

    question: str    = InputField(desc="Original management question in Thai or English")
    table_view: str  = InputField(desc="SQL result as a small markdown table")

    kpi_summary: str = OutputField(desc="Short bullet list of key KPIs in Thai (B1)")
    explanation: str = OutputField(desc="Insight explanation in Thai (B1)")
    action: str      = OutputField(desc="1–3 recommended actions in Thai (B1)")


insight_predictor = dspy.Predict(InsightFromResult)


def generate_insight(question: str, table_view: str):
    return insight_predictor(question=question, table_view=table_view)


# ============================================
# 5) MAIN ENTRY FOR APP: ask_bot_core
# ============================================

def ask_bot_core(question: str) -> dict:
    """
    ฟังก์ชัน core สำหรับใช้ใน Streamlit / API:
    - รับคำถามผู้บริหาร (ภาษาไทย/อังกฤษ)
    - ใช้ optimized_planner สร้าง SQL
    - รัน SQL กับ DuckDB
    - แปลงผลลัพธ์เป็น KPI + Explanation + Action
    - คืนเป็น dict อย่างเดียว (ไม่ print อะไร)
    """
    plan = optimized_planner(question)
    raw_sql = plan.sql
    sql = clean_sql(raw_sql)

    df, table_view = run_sql(sql)

    if df.empty:
        return {
            "question": question,
            "intent": getattr(plan, "intent", ""),
            "sql": sql,
            "table_view": table_view,
            "kpi_summary": "",
            "explanation": "ไม่มีข้อมูลในช่วง / เงื่อนไขนี้",
            "action": "ลองปรับคำถาม หรือช่วงวันที่ใหม่อีกครั้ง",
        }

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
