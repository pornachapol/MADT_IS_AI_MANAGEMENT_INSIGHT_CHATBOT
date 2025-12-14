# core.py
# Core logic for iPhone Gold Datamart Insight Chatbot
# FIXED VERSION – Proper DSPy LM configuration

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
    """ดึง GEMINI_API_KEY จาก Streamlit secrets หรือ env แล้วตั้งค่า env ให้พร้อมใช้"""
    try:
        import streamlit as st
        if "GEMINI_API_KEY" in st.secrets:
            os.environ["GEMINI_API_KEY"] = st.secrets["GEMINI_API_KEY"]
    except Exception:
        # ไม่ได้รันผ่าน Streamlit หรือไม่มี secrets
        pass

    if "GEMINI_API_KEY" not in os.environ:
        raise ValueError("GEMINI_API_KEY not found. Please set it in Streamlit secrets or environment variables.")


def ensure_lm_configured():
    """
    Ensure LM is configured with thread-safe caching.
    Only configure ONCE per thread - never reconfigure.
    """
    configure_api_key()
    
    try:
        import streamlit as st
        
        @st.cache_resource(show_spinner=False)
        def _configure_lm_once():
            """Configure LM exactly once - cached forever"""
            lm = dspy.LM("gemini/gemini-2.5-flash")
            dspy.configure(lm=lm)
            return True  # Just return a flag
        
        # This will only run once and be cached
        _configure_lm_once()
        
        # DO NOT call dspy.configure() again!
        # The cache ensures it was already called once
        
    except ImportError:
        # Not running in Streamlit
        lm = dspy.LM("gemini/gemini-2.5-flash")
        dspy.configure(lm=lm)


# ============================================
# 2) INITIALIZE DUCKDB
# ============================================

def ensure_database_exists():
    """Ensure DuckDB database exists"""
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
            if os.path.exists(DB_PATH):
                os.remove(DB_PATH)
            from init_db import init_database
            init_database(DB_PATH)


ensure_database_exists()


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

    Rules:
    - ใช้เฉพาะตาราง:
      fact_registration(date_key, branch_id, product_id, reg_count)
      fact_contract(date_key, branch_id, product_id, contract_count)
      fact_inventory_snapshot(date_key, branch_id, product_id, stock_qty)
      dim_date(date_key, date, year, month, day)
      dim_product(product_id, model_name, generation, storage_gb, color, base_price)
      dim_branch(branch_id, branch_code, branch_name, branch_type, province, is_active)

    - วันที่: date_key = INT YYYYMMDD
    - Revenue = SUM(c.contract_count * p.base_price) ถ้าถามยอดขายเป็นเงิน
    - ใช้ province ไม่ใช่ region สำหรับ dim_branch
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
# 5) TRAINSET (9 EXAMPLES)
# ============================================

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
    """
).with_inputs("question")

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
    """
).with_inputs("question")

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
    """
).with_inputs("question")

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
    """
).with_inputs("question")

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
    """
).with_inputs("question")

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
    """
).with_inputs("question")

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
    """
).with_inputs("question")

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
    """
).with_inputs("question")

ex9 = dspy.Example(
    question="เดือน 11 ปี 2025 เทียบกับเดือน 10 ปี 2025 ยอดขายเป็นเงินรวมเป็นยังไง?",
    intent="monthly_revenue_vs_prev_month",
    sql="""
        WITH monthly_revenue AS (
            SELECT
                d.year,
                d.month,
                SUM(c.contract_count * p.base_price) AS total_revenue
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
    """
).with_inputs("question")

ex10 = dspy.Example(
    question="ตอนนี้เราขาดโอกาสการขายที่ไหนบ้าง? มี SKU ไหนที่ลูกค้าต้องการแต่สต็อกไม่พอ?",
    intent="lost_opportunity_current",
    sql="""
        WITH latest_date AS (
            SELECT MAX(date_key) as max_date
            FROM fact_registration
        ),
        lost_opp AS (
            SELECT
                b.branch_code,
                b.branch_name,
                b.province,
                p.generation AS iphone_gen,
                p.storage_gb,
                p.color,
                SUM(r.reg_count) AS total_demand,
                SUM(COALESCE(i.stock_qty, 0)) AS current_stock,
                SUM(r.reg_count) - SUM(COALESCE(i.stock_qty, 0)) AS lost_opportunity,
                (SUM(r.reg_count) - SUM(COALESCE(i.stock_qty, 0))) * AVG(p.base_price) AS lost_revenue_estimate
            FROM fact_registration r
            JOIN dim_branch b ON r.branch_id = b.branch_id
            JOIN dim_product p ON r.product_id = p.product_id
            LEFT JOIN fact_inventory_snapshot i 
                ON r.date_key = i.date_key 
                AND r.branch_id = i.branch_id 
                AND r.product_id = i.product_id
            CROSS JOIN latest_date ld
            WHERE r.date_key = ld.max_date
              AND b.branch_type = 'SHOP'
            GROUP BY b.branch_code, b.branch_name, b.province, p.generation, p.storage_gb, p.color
            HAVING SUM(r.reg_count) > SUM(COALESCE(i.stock_qty, 0))
        )
        SELECT
            branch_code,
            branch_name,
            province,
            iphone_gen,
            storage_gb,
            color,
            total_demand,
            current_stock,
            lost_opportunity,
            ROUND(lost_revenue_estimate, 0) AS lost_revenue_baht,
            ROUND(lost_opportunity * 100.0 / total_demand, 1) AS stockout_rate_pct
        FROM lost_opp
        ORDER BY lost_opportunity DESC
        LIMIT 20;
    """
).with_inputs("question")

ex11 = dspy.Example(
    question="เติมของสาขาไหนจะทำให้ยอดขายเพิ่มขึ้นมากที่สุด? สาขาไหนควรเติมของด่วน?",
    intent="restocking_priority",
    sql="""
        WITH latest_date AS (
            SELECT MAX(date_key) as max_date
            FROM fact_registration
        ),
        recent_7days AS (
            SELECT MAX(date_key) - 6 as start_date
            FROM fact_registration
        ),
        demand_pattern AS (
            SELECT
                r.branch_id,
                r.product_id,
                AVG(r.reg_count) AS avg_daily_demand,
                SUM(COALESCE(c.contract_count, 0)) AS actual_sales_7d,
                SUM(r.reg_count) AS total_demand_7d,
                CASE 
                    WHEN SUM(r.reg_count) > 0 
                    THEN ROUND(SUM(COALESCE(c.contract_count, 0)) * 100.0 / SUM(r.reg_count), 1)
                    ELSE 0 
                END AS conversion_rate
            FROM fact_registration r
            LEFT JOIN fact_contract c 
                ON r.date_key = c.date_key 
                AND r.branch_id = c.branch_id 
                AND r.product_id = c.product_id
            CROSS JOIN recent_7days rd
            WHERE r.date_key >= rd.start_date
            GROUP BY r.branch_id, r.product_id
        ),
        current_inventory AS (
            SELECT
                i.branch_id,
                i.product_id,
                i.stock_qty
            FROM fact_inventory_snapshot i
            CROSS JOIN latest_date ld
            WHERE i.date_key = ld.max_date
        ),
        restock_priority AS (
            SELECT
                b.branch_code,
                b.branch_name,
                b.province,
                p.generation AS iphone_gen,
                p.storage_gb,
                p.color,
                ROUND(dp.avg_daily_demand, 1) AS avg_daily_demand,
                COALESCE(ci.stock_qty, 0) AS current_stock,
                ROUND(COALESCE(ci.stock_qty, 0) / NULLIF(dp.avg_daily_demand, 0), 1) AS days_of_stock,
                dp.conversion_rate,
                dp.actual_sales_7d,
                dp.total_demand_7d - dp.actual_sales_7d AS missed_sales_7d,
                CASE
                    WHEN COALESCE(ci.stock_qty, 0) = 0 THEN 'STOCKOUT'
                    WHEN COALESCE(ci.stock_qty, 0) / NULLIF(dp.avg_daily_demand, 0) < 3 THEN 'CRITICAL'
                    WHEN COALESCE(ci.stock_qty, 0) / NULLIF(dp.avg_daily_demand, 0) < 7 THEN 'LOW'
                    ELSE 'OK'
                END AS stock_status,
                CEIL(GREATEST(0, dp.avg_daily_demand * 14 - COALESCE(ci.stock_qty, 0))) AS recommended_restock,
                CEIL(GREATEST(0, dp.avg_daily_demand * 14 - COALESCE(ci.stock_qty, 0))) * p.base_price AS potential_revenue
            FROM demand_pattern dp
            JOIN dim_branch b ON dp.branch_id = b.branch_id
            JOIN dim_product p ON dp.product_id = p.product_id
            LEFT JOIN current_inventory ci ON dp.branch_id = ci.branch_id AND dp.product_id = ci.product_id
            WHERE b.branch_type = 'SHOP'
              AND dp.avg_daily_demand > 0.5
        )
        SELECT
            branch_code,
            branch_name,
            province,
            iphone_gen,
            storage_gb,
            color,
            stock_status,
            avg_daily_demand,
            current_stock,
            days_of_stock,
            conversion_rate,
            recommended_restock,
            potential_revenue
        FROM restock_priority
        WHERE stock_status IN ('STOCKOUT', 'CRITICAL', 'LOW')
        ORDER BY 
            CASE stock_status 
                WHEN 'STOCKOUT' THEN 1 
                WHEN 'CRITICAL' THEN 2 
                WHEN 'LOW' THEN 3 
            END,
            potential_revenue DESC
        LIMIT 30;
    """
).with_inputs("question")

ex12 = dspy.Example(
    question="สาขาไหนมี conversion rate ดีที่สุด และสาขาไหนควรปรับปรุง?",
    intent="branch_conversion_performance",
    sql="""
        WITH recent_7days AS (
            SELECT MAX(date_key) - 6 as start_date
            FROM fact_registration
        ),
        branch_perf AS (
            SELECT
                b.branch_code,
                b.branch_name,
                b.province,
                SUM(r.reg_count) AS total_registrations,
                SUM(COALESCE(c.contract_count, 0)) AS total_contracts,
                CASE 
                    WHEN SUM(r.reg_count) = 0 THEN 0
                    ELSE ROUND(SUM(COALESCE(c.contract_count, 0)) * 100.0 / SUM(r.reg_count), 1)
                END AS conversion_rate,
                SUM(COALESCE(c.contract_count, 0) * p.base_price) AS total_revenue
            FROM fact_registration r
            JOIN dim_branch b ON r.branch_id = b.branch_id
            LEFT JOIN fact_contract c 
                ON r.date_key = c.date_key 
                AND r.branch_id = c.branch_id 
                AND r.product_id = c.product_id
            LEFT JOIN dim_product p ON r.product_id = p.product_id
            CROSS JOIN recent_7days rd
            WHERE r.date_key >= rd.start_date
              AND b.branch_type = 'SHOP'
            GROUP BY b.branch_code, b.branch_name, b.province
        )
        SELECT
            branch_code,
            branch_name,
            province,
            total_registrations,
            total_contracts,
            conversion_rate,
            total_revenue,
            CASE
                WHEN conversion_rate >= 60 THEN 'EXCELLENT'
                WHEN conversion_rate >= 50 THEN 'GOOD'
                WHEN conversion_rate >= 40 THEN 'AVERAGE'
                ELSE 'NEEDS_IMPROVEMENT'
            END AS performance_tier
        FROM branch_perf
        ORDER BY conversion_rate DESC;
    """
).with_inputs("question")

ex13 = dspy.Example(
    question="สาขาไหนมีสต็อกพอขายกี่วัน? สาขาไหนจะหมดสต็อกเร็วที่สุด?",
    intent="inventory_days_supply",
    sql="""
        WITH latest_date AS (
            SELECT MAX(date_key) as max_date
            FROM fact_registration
        ),
        recent_7days AS (
            SELECT MAX(date_key) - 6 as start_date
            FROM fact_registration
        ),
        avg_demand AS (
            SELECT
                r.branch_id,
                r.product_id,
                AVG(r.reg_count) AS avg_daily_demand
            FROM fact_registration r
            CROSS JOIN recent_7days rd
            WHERE r.date_key >= rd.start_date
            GROUP BY r.branch_id, r.product_id
        ),
        current_stock AS (
            SELECT
                i.branch_id,
                i.product_id,
                i.stock_qty
            FROM fact_inventory_snapshot i
            CROSS JOIN latest_date ld
            WHERE i.date_key = ld.max_date
        )
        SELECT
            b.branch_code,
            b.branch_name,
            p.generation AS iphone_gen,
            p.storage_gb,
            cs.stock_qty AS current_stock,
            ROUND(ad.avg_daily_demand, 1) AS avg_daily_demand,
            ROUND(cs.stock_qty / NULLIF(ad.avg_daily_demand, 0), 1) AS days_of_supply,
            CASE
                WHEN cs.stock_qty = 0 THEN 'OUT_OF_STOCK'
                WHEN cs.stock_qty / NULLIF(ad.avg_daily_demand, 0) < 3 THEN 'CRITICAL'
                WHEN cs.stock_qty / NULLIF(ad.avg_daily_demand, 0) < 7 THEN 'LOW'
                WHEN cs.stock_qty / NULLIF(ad.avg_daily_demand, 0) < 14 THEN 'NORMAL'
                ELSE 'EXCESS'
            END AS inventory_status
        FROM current_stock cs
        JOIN avg_demand ad ON cs.branch_id = ad.branch_id AND cs.product_id = ad.product_id
        JOIN dim_branch b ON cs.branch_id = b.branch_id
        JOIN dim_product p ON cs.product_id = p.product_id
        WHERE b.branch_type = 'SHOP'
          AND ad.avg_daily_demand > 0.5
        ORDER BY days_of_supply ASC, avg_daily_demand DESC;
    """
).with_inputs("question")

trainset = [ex1, ex2, ex3, ex4, ex5, ex6, ex7, ex8, ex9, ex10, ex11, ex12, ex13]


def get_optimized_planner():
    """
    Get optimized planner with proper caching using Streamlit.
    This ensures the planner is compiled once and reused across requests.
    """
    ensure_lm_configured()
    
    # Try to use Streamlit's cache if available
    try:
        import streamlit as st
        
        @st.cache_resource
        def _cached_compile():
            teleprompter = BootstrapFewShot(metric=lambda ex, pred, trace=None: 0.0)
            return teleprompter.compile(SQLPlanner(), trainset=trainset)
        
        return _cached_compile()
        
    except ImportError:
        # Not running in Streamlit, use simple compilation
        teleprompter = BootstrapFewShot(metric=lambda ex, pred, trace=None: 0.0)
        return teleprompter.compile(SQLPlanner(), trainset=trainset)


# ============================================
# 6) INSIGHT LAYER
# ============================================

class InsightFromResult(dspy.Signature):
    """Turn a SQL result table into management insight and actions in Thai (B1)."""
    question: str = InputField()
    table_view: str = InputField()
    kpi_summary: str = OutputField()
    explanation: str = OutputField()
    action: str = OutputField()


def get_insight_predictor():
    """Get insight predictor with LM configured"""
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
    ฟังก์ชัน core สำหรับใช้ใน Streamlit / API:
    - รับคำถามผู้บริหาร (ภาษาไทย/อังกฤษ)
    - ใช้ optimized_planner สร้าง SQL
    - รัน SQL กับ DuckDB
    - แปลงผลลัพธ์เป็น KPI + Explanation + Action
    - คืนเป็น dict อย่างเดียว (ไม่ print อะไร)
    """
    
    # Ensure LM is configured
    ensure_lm_configured()
    
    # Get the optimized planner (lazy init)
    planner = get_optimized_planner()

    # 1) ให้ DSPy วางแผน Intent + SQL
    plan = planner(question)
    raw_sql = plan.sql
    sql = clean_sql(raw_sql)

    # 2) รัน SQL กับ DuckDB
    df, table_view = run_sql(sql)

    # 3) ถ้าไม่มีข้อมูล ให้ตอบแบบ graceful
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

    # 4) ให้ LLM สรุปอินไซต์จากตาราง
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
