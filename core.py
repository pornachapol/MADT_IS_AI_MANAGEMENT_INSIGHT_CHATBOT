# core.py
# Core logic for iPhone Gold Datamart Insight Chatbot
# OPTIMIZED VERSION - Free Tier Performance

import os
import re
import duckdb
import pandas as pd
import dspy
from dspy import InputField, OutputField
import json
from typing import Optional

# ============================================
# 0) CONFIG & CONSTANTS
# ============================================

DB_PATH = "iphone_gold.duckdb"
COMPILED_PROGRAM_PATH = "optimized_planner.json"

# Global variable to track if LM is configured
_lm_configured = False
_db_connection = None

# ============================================
# 1) LLM CONFIG (DSPy + GEMINI)
# ============================================

def configure_api_key():
    """ดึง GEMINI_API_KEY จาก Streamlit secrets หรือ env"""
    try:
        import streamlit as st
        if "GEMINI_API_KEY" in st.secrets:
            os.environ["GEMINI_API_KEY"] = st.secrets["GEMINI_API_KEY"]
    except Exception:
        pass

    if "GEMINI_API_KEY" not in os.environ:
        raise ValueError("GEMINI_API_KEY not found. Please set it in Streamlit secrets or environment variables.")


def ensure_lm_configured():
    """Ensure LM is configured before use"""
    global _lm_configured
    
    if not _lm_configured:
        configure_api_key()
        # ใช้ Gemini 1.5 Flash (stable, free tier)
        lm = dspy.LM(
            "gemini/gemini-1.5-flash",
            temperature=0.0  # Deterministic สำหรับ SQL
        )
        dspy.configure(lm=lm)
        _lm_configured = True


# ============================================
# 2) INITIALIZE DUCKDB WITH CONNECTION POOLING
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


def get_db_connection():
    """Get persistent DuckDB connection (reuse connection)"""
    global _db_connection
    if _db_connection is None:
        ensure_database_exists()
        _db_connection = duckdb.connect(DB_PATH, read_only=True)
    return _db_connection


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


def run_sql(sql: str):
    """รัน SQL กับ DuckDB แล้วคืน (DataFrame, markdown-table-string) - ใช้ persistent connection"""
    try:
        con = get_db_connection()
        df = con.execute(sql).df()

        if df.empty:
            table_view = "*(no rows)*"
        else:
            table_view = df.to_markdown(index=False)

        return df, table_view
    except Exception as e:
        raise Exception(f"SQL Error: {str(e)}\nSQL: {sql}")


# ============================================
# 4) DSPy SIGNATURES & MODULES (SIMPLIFIED)
# ============================================

class IntentAndSQL(dspy.Signature):
    """
    Convert a top-management business question into DuckDB SQL using the iPhone Gold Datamart.

    Tables:
    - fact_registration(date_key, branch_id, product_id, reg_count)
    - fact_contract(date_key, branch_id, product_id, contract_count)
    - fact_inventory_snapshot(date_key, branch_id, product_id, stock_qty)
    - dim_date(date_key, date, year, month, day)
    - dim_product(product_id, model_name, generation, storage_gb, color, base_price)
    - dim_branch(branch_id, branch_code, branch_name, region)

    Rules:
    - date_key = INT YYYYMMDD format
    - Revenue = SUM(contract_count * base_price)
    
    Example 1 - Best selling model:
    Q: "เดือน 11 ปี 2025 รุ่น iPhone ไหนขายดีที่สุด?"
    SQL: SELECT p.generation, SUM(c.contract_count) AS units
         FROM fact_contract c JOIN dim_product p ON c.product_id = p.product_id
         JOIN dim_date d ON c.date_key = d.date_key
         WHERE d.year = 2025 AND d.month = 11
         GROUP BY p.generation ORDER BY units DESC;
    
    Example 2 - Conversion rate:
    Q: "Conversion Rate ของแต่ละสาขาในเดือน 11 ปี 2025"
    SQL: SELECT b.branch_name, 
         ROUND(SUM(c.contract_count) * 1.0 / SUM(r.reg_count), 2) AS conv_rate
         FROM fact_registration r JOIN dim_branch b ON r.branch_id = b.branch_id
         JOIN dim_date d ON r.date_key = d.date_key
         LEFT JOIN fact_contract c ON r.date_key = c.date_key 
         AND r.branch_id = c.branch_id AND r.product_id = c.product_id
         WHERE d.year = 2025 AND d.month = 11
         GROUP BY b.branch_name ORDER BY conv_rate DESC;
    
    Example 3 - Lost opportunity:
    Q: "วันที่ 11/11/2025 สาขาไหนเสียโอกาสขาย (Demand > Stock) สูงสุด?"
    SQL: SELECT b.branch_name, SUM(r.reg_count - i.stock_qty) AS lost_opp
         FROM fact_registration r JOIN fact_inventory_snapshot i
         ON r.date_key = i.date_key AND r.branch_id = i.branch_id 
         AND r.product_id = i.product_id
         JOIN dim_branch b ON r.branch_id = b.branch_id
         WHERE r.date_key = 20251111
         GROUP BY b.branch_name HAVING SUM(r.reg_count) > SUM(i.stock_qty)
         ORDER BY lost_opp DESC;
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
# 5) OPTIMIZED PLANNER WITH FILE CACHE
# ============================================

_optimized_planner = None


def get_optimized_planner():
    """
    Lazy initialization - ใช้ ChainOfThought โดยตรง
    ไม่ต้อง compile (เหมาะสำหรับ online deployment)
    """
    global _optimized_planner
    
    if _optimized_planner is None:
        ensure_lm_configured()
        print("ℹ️ Using ChainOfThought planner (no compilation needed)")
        _optimized_planner = SQLPlanner()
    
    return _optimized_planner


# ============================================
# 6) TEMPLATE-BASED INSIGHT (NO LLM CALL)
# ============================================

INSIGHT_TEMPLATES = {
    "best_selling_model_mtd": {
        "kpi": "รุ่น {top_model} ขายดีที่สุด {top_units} เครื่อง",
        "explanation": "จากข้อมูลยอดขายเครื่อง พบว่า iPhone {top_model} มียอดสูงสุด ซึ่งแสดงถึงความนิยมของรุ่นนี้ในช่วงเวลาดังกล่าว",
        "action": "1) เพิ่มสต็อก iPhone {top_model} ให้เพียงพอ\n2) จัด promotion ต่อเนื่องสำหรับรุ่นนี้\n3) Training พนักงานขายให้เชี่ยวชาญรุ่นนี้"
    },
    "best_branch_mtd": {
        "kpi": "สาขา {top_branch} ทำยอดสูงสุด {top_units} เครื่อง",
        "explanation": "สาขา {top_branch} มีผลงานโดดเด่น อาจเนื่องมาจากทำเลที่ดี ทีมขายเก่ง หรือกลยุทธ์ที่เหมาะสม",
        "action": "1) ศึกษา Best Practice จากสาขานี้\n2) นำไปถ่ายทอดให้สาขาอื่น\n3) Reward ทีมที่ทำงานดี"
    },
    "branch_conversion_mtd": {
        "kpi": "Conversion Rate เฉลี่ย {avg_rate}%",
        "explanation": "สาขาที่มี conversion สูง แสดงว่าทีมขายปิดการขายได้ดี ในขณะที่สาขาที่ต่ำอาจต้องการการ support",
        "action": "1) สาขาที่ conversion ต่ำ: เพิ่ม training ทักษะการขาย\n2) ตรวจสอบคุณภาพของ lead ที่ลงทะเบียน\n3) Share best practice จากสาขาที่ conversion สูง"
    },
    "lost_opportunity_by_branch_on_date": {
        "kpi": "สาขา {top_branch} เสียโอกาส {lost_units} เครื่อง",
        "explanation": "มีความต้องการสูงแต่สต็อกไม่พอ ทำให้เสียโอกาสในการทำยอดขาย ซึ่งส่งผลต่อ revenue และความพึงพอใจของลูกค้า",
        "action": "1) Transfer สต็อกด่วนไปสาขานี้\n2) ปรับระบบ forecasting และ replenishment\n3) ติดตาม demand pattern เพื่อป้องกันในอนาคต"
    },
    "daily_sales_trend_mtd": {
        "kpi": "ยอดขายเฉลี่ย {avg_daily} เครื่อง/วัน",
        "explanation": "แนวโน้มยอดขายต่อวันช่วยวางแผน inventory และ staffing ในแต่ละช่วงเวลา",
        "action": "1) วันที่ยอดสูง: เพิ่ม staff และเตรียม stock\n2) วันที่ยอดต่ำ: จัด promotion หรือ marketing campaign\n3) วิเคราะห์ pattern เพื่อ optimize operation"
    },
    "demand_by_generation_mtd": {
        "kpi": "ความต้องการสูงสุดคือรุ่น {top_gen}",
        "explanation": "ข้อมูล registration แสดงความสนใจของลูกค้า ซึ่งอาจต่างจากยอดขายจริง (ขึ้นอยู่กับสต็อกและ conversion)",
        "action": "1) รุ่นที่มี demand สูง: เตรียมสต็อกให้เพียงพอ\n2) รุ่นที่ demand ต่ำ: พิจารณา promotion\n3) วิเคราะห์ gap ระหว่าง demand vs actual sales"
    },
    "monthly_revenue_vs_prev_month": {
        "kpi": "Revenue เดือนนี้ {current_rev:,.0f} บาท ({growth:+.1f}%)",
        "explanation": "การเปรียบเทียบ month-over-month ช่วยประเมินว่า business กำลังเติบโตหรือหดตัว และควรปรับกลยุทธ์อย่างไร",
        "action": "1) ถ้าโต: รักษา momentum ด้วยการ sustain กลยุทธ์ปัจจุบัน\n2) ถ้าหด: วิเคราะห์สาเหตุและปรับแผน\n3) เปรียบเทียบกับ target เพื่อ course correction"
    }
}


def generate_template_insight(intent: str, df: pd.DataFrame) -> Optional[dict]:
    """
    Generate insight from template (no LLM call)
    Return None if template not available or data doesn't match expected format
    """
    if intent not in INSIGHT_TEMPLATES:
        return None
    
    template = INSIGHT_TEMPLATES[intent]
    
    try:
        if intent in ["best_selling_model_mtd", "Best selling model"]:
            if df.empty:
                return None
            top_row = df.iloc[0]
            # Find generation column (flexible matching)
            gen_cols = [c for c in df.columns if 'gen' in c.lower() or 'model' in c.lower()]
            units_cols = [c for c in df.columns if 'unit' in c.lower() or 'count' in c.lower()]
            
            if not gen_cols or not units_cols:
                return None
                
            gen_col = gen_cols[0]
            units_col = units_cols[0]
            
            return {
                "kpi_summary": template["kpi"].format(
                    top_model=top_row[gen_col],
                    top_units=int(top_row[units_col])
                ),
                "explanation": template["explanation"].format(
                    top_model=top_row[gen_col]
                ),
                "action": template["action"].format(
                    top_model=top_row[gen_col]
                )
            }
        
        elif intent in ["best_branch_mtd", "Best branch"]:
            if df.empty:
                return None
            top_row = df.iloc[0]
            # Flexible column matching
            branch_cols = [c for c in df.columns if 'branch' in c.lower() and ('name' in c.lower() or 'code' in c.lower())]
            units_cols = [c for c in df.columns if 'unit' in c.lower() or 'sold' in c.lower() or 'count' in c.lower()]
            
            if not branch_cols or not units_cols:
                return None
                
            branch_col = branch_cols[0]
            units_col = units_cols[0]
            
            return {
                "kpi_summary": template["kpi"].format(
                    top_branch=top_row[branch_col],
                    top_units=int(top_row[units_col])
                ),
                "explanation": template["explanation"].format(
                    top_branch=top_row[branch_col]
                ),
                "action": template["action"]
            }
        
        elif intent in ["branch_conversion_mtd", "Conversion rate"]:
            if df.empty:
                return None
            # Flexible column matching
            rate_cols = [c for c in df.columns if 'conv' in c.lower() or 'rate' in c.lower()]
            
            if not rate_cols:
                return None
                
            rate_col = rate_cols[0]
            avg_rate = df[rate_col].mean() * 100
            
            return {
                "kpi_summary": template["kpi"].format(avg_rate=f"{avg_rate:.1f}"),
                "explanation": template["explanation"],
                "action": template["action"]
            }
        
        elif intent in ["lost_opportunity_by_branch_on_date", "lost_opportunity", "Lost opportunity"]:
            if df.empty:
                return None
            top_row = df.iloc[0]
            # Flexible column matching
            branch_cols = [c for c in df.columns if 'branch' in c.lower()]
            lost_cols = [c for c in df.columns if 'lost' in c.lower() or 'opp' in c.lower()]
            
            if not branch_cols or not lost_cols:
                return None
                
            branch_col = branch_cols[0]
            lost_col = lost_cols[0]
            
            return {
                "kpi_summary": template["kpi"].format(
                    top_branch=top_row[branch_col],
                    lost_units=int(top_row[lost_col])
                ),
                "explanation": template["explanation"],
                "action": template["action"]
            }
        
        elif intent in ["daily_sales_trend_mtd", "Daily sales"]:
            if df.empty:
                return None
            # Flexible column matching
            units_cols = [c for c in df.columns if 'unit' in c.lower() or 'sold' in c.lower() or 'count' in c.lower()]
            
            if not units_cols:
                return None
                
            units_col = units_cols[0]
            avg_daily = df[units_col].mean()
            
            return {
                "kpi_summary": template["kpi"].format(avg_daily=f"{avg_daily:.1f}"),
                "explanation": template["explanation"],
                "action": template["action"]
            }
        
        elif intent in ["demand_by_generation_mtd", "Demand by generation"]:
            if df.empty:
                return None
            top_row = df.iloc[0]
            # Flexible column matching
            gen_cols = [c for c in df.columns if 'gen' in c.lower() or 'model' in c.lower()]
            
            if not gen_cols:
                return None
                
            gen_col = gen_cols[0]
            
            return {
                "kpi_summary": template["kpi"].format(
                    top_gen=top_row[gen_col]
                ),
                "explanation": template["explanation"],
                "action": template["action"]
            }
        
        elif intent in ["monthly_revenue_vs_prev_month", "Monthly revenue comparison"]:
            if df.empty:
                return None
            row = df.iloc[0]
            # Flexible column matching
            rev_cols = [c for c in df.columns if 'rev' in c.lower() and 'current' in c.lower()]
            growth_cols = [c for c in df.columns if 'growth' in c.lower() or 'pct' in c.lower()]
            
            if not rev_cols:
                return None
                
            rev_col = rev_cols[0]
            growth_col = growth_cols[0] if growth_cols else None
            
            growth_val = row[growth_col] if growth_col else 0
            
            return {
                "kpi_summary": template["kpi"].format(
                    current_rev=row[rev_col],
                    growth=growth_val
                ),
                "explanation": template["explanation"],
                "action": template["action"]
            }
        
    except Exception as e:
        # If template fails, return None to fallback to LLM
        print(f"⚠️ Template generation failed: {e}")
        return None
    
    return None


# ============================================
# 7) SIMPLIFIED INSIGHT LAYER (FALLBACK)
# ============================================

class InsightFromResult(dspy.Signature):
    """Turn a SQL result table into Thai management insight (single output)."""
    question: str = InputField()
    table_view: str = InputField()
    insight: str = OutputField(desc="รวม KPI, คำอธิบาย และ Action ในข้อความเดียว")


def get_insight_predictor():
    """Get insight predictor with LM configured"""
    ensure_lm_configured()
    return dspy.Predict(InsightFromResult)


def generate_insight_llm(question: str, table_view: str) -> dict:
    """Generate insight using LLM (fallback method)"""
    predictor = get_insight_predictor()
    result = predictor(question=question, table_view=table_view)
    
    # Parse the combined insight into components
    insight_text = result.insight
    
    # Simple heuristic to split into sections
    parts = insight_text.split("\n\n")
    if len(parts) >= 3:
        return {
            "kpi_summary": parts[0],
            "explanation": parts[1],
            "action": "\n".join(parts[2:])
        }
    else:
        return {
            "kpi_summary": "",
            "explanation": insight_text,
            "action": ""
        }


# ============================================
# 8) MAIN ENTRY FOR APP
# ============================================

def ask_bot_core(question: str) -> dict:
    """
    Optimized core function:
    - Uses cached compiled program (if available)
    - Template-based insights (no LLM) for common queries
    - Falls back to LLM only when needed
    - Reuses DB connection
    """
    
    # Ensure LM is configured
    ensure_lm_configured()
    
    # Get the optimized planner (lazy init with file cache)
    planner = get_optimized_planner()

    # 1) Generate SQL
    plan = planner(question)
    raw_sql = plan.sql
    sql = clean_sql(raw_sql)
    intent = getattr(plan, "intent", "")

    # 2) Run SQL
    df, table_view = run_sql(sql)

    # 3) If no data, return gracefully
    if df.empty:
        return {
            "question": question,
            "intent": intent,
            "sql": sql,
            "table_view": table_view,
            "kpi_summary": "",
            "explanation": "ไม่พบข้อมูลในเงื่อนไขนี้",
            "action": "ลองเปลี่ยนเดือน / ปี หรือเงื่อนไขดูอีกครั้ง",
        }

    # 4) Try template-based insight first (fast, no API call)
    template_insight = generate_template_insight(intent, df)
    
    if template_insight:
        print("✅ Using template-based insight (no LLM call)")
        return {
            "question": question,
            "intent": intent,
            "sql": sql,
            "table_view": table_view,
            **template_insight
        }
    
    # 5) Fallback to LLM-based insight
    print("🤖 Using LLM-based insight (custom query)")
    llm_insight = generate_insight_llm(question=question, table_view=table_view)
    
    return {
        "question": question,
        "intent": intent,
        "sql": sql,
        "table_view": table_view,
        **llm_insight
    }
