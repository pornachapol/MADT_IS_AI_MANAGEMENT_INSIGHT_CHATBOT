"""
compile_app.py
==============
Streamlit app สำหรับ compile planner online ผ่าน Streamlit Cloud

วิธีใช้:
1. เพิ่มไฟล์นี้ใน repo
2. Deploy เป็น Streamlit app ชั่วคราว
3. กดปุ่ม "Compile Planner"
4. Download ไฟล์ optimized_planner.json
5. Commit ไฟล์นั้นกลับเข้า repo
6. ลบ compile_app.py ทิ้ง (หรือเก็บไว้ใช้ภายหลัง)
"""

import streamlit as st
import os
import dspy
from dspy import InputField, OutputField
from dspy.teleprompt import BootstrapFewShot
import json

st.set_page_config(page_title="DSPy Planner Compiler", layout="wide")

st.title("🔨 DSPy Planner Compiler")
st.caption("Compile optimized planner online ผ่าน Streamlit Cloud")

# ============================================
# CHECK API KEY
# ============================================

if "GEMINI_API_KEY" not in st.secrets and "GEMINI_API_KEY" not in os.environ:
    st.error("⚠️ **GEMINI_API_KEY not found!**")
    st.info("Please add your Gemini API key in Streamlit Cloud Settings → Secrets")
    st.code('GEMINI_API_KEY = "your-api-key-here"', language="toml")
    st.stop()

# Set from secrets
if "GEMINI_API_KEY" in st.secrets:
    os.environ["GEMINI_API_KEY"] = st.secrets["GEMINI_API_KEY"]

st.success("✅ GEMINI_API_KEY found")

# ============================================
# DEFINE SIGNATURES & MODULES
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
# TRAINSET
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

ex3 = dspy.Example(
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

ex4 = dspy.Example(
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

ex5 = dspy.Example(
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

trainset = [ex1, ex2, ex3, ex4, ex5]

# ============================================
# UI
# ============================================

st.info(f"📚 Trainset ready with {len(trainset)} examples")

st.markdown("---")

if st.button("🔨 Compile Planner Now", type="primary"):
    
    with st.spinner("🔄 Compiling planner (this may take 1-2 minutes)..."):
        
        try:
            # Configure DSPy
            progress = st.progress(0)
            st.write("⚙️ Step 1/4: Configuring DSPy...")
            lm = dspy.LM(
                "gemini/gemini-2.5-flash",  # ← ใหม่
                temperature=0.0
            )
            dspy.configure(lm=lm)
            progress.progress(25)
            
            # Create planner
            st.write("🏗️ Step 2/4: Creating base planner...")
            base_planner = SQLPlanner()
            progress.progress(50)
            
            # Compile
            st.write("🔨 Step 3/4: Compiling with BootstrapFewShot...")
            teleprompter = BootstrapFewShot(
                metric=lambda ex, pred, trace=None: 0.0,
                max_bootstrapped_demos=3,
                max_labeled_demos=3
            )
            optimized_planner = teleprompter.compile(base_planner, trainset=trainset)
            progress.progress(75)
            
            # Save to temp file
            st.write("💾 Step 4/4: Saving compiled planner...")
            output_file = "optimized_planner.json"
            optimized_planner.save(output_file)
            progress.progress(100)
            
            st.success("✅ Compilation complete!")
            
            # ============================================
            # DOWNLOAD BUTTON
            # ============================================
            
            st.markdown("---")
            st.subheader("📥 Download Compiled File")
            
            with open(output_file, "r") as f:
                file_content = f.read()
            
            st.download_button(
                label="⬇️ Download optimized_planner.json",
                data=file_content,
                file_name="optimized_planner.json",
                mime="application/json",
                type="primary"
            )
            
            # Show file info
            file_size = len(file_content)
            st.info(f"📊 File size: {file_size:,} bytes ({file_size/1024:.1f} KB)")
            
            # ============================================
            # NEXT STEPS
            # ============================================
            
            st.markdown("---")
            st.subheader("📋 Next Steps")
            
            st.markdown("""
            **หลังจาก download แล้ว:**
            
            1. **ลบ compile_app.py ออกจาก repo** (ไม่ต้องใช้แล้ว)
            2. **เพิ่ม optimized_planner.json เข้า repo:**
               ```bash
               git add optimized_planner.json
               git commit -m "Add pre-compiled planner"
               git push
               ```
            3. **Deploy app หลัก (app.py)** - จะโหลด planner ทันที ไม่ต้อง compile
            4. **ผลลัพธ์:** Cold start 60-120s → 2-5s (95% เร็วขึ้น!) 🚀
            
            **หมายเหตุ:**
            - ไฟล์นี้ใช้ได้ตลอด ไม่ต้อง compile ใหม่
            - Compile ใหม่ก็ต่อเมื่อแก้ trainset เท่านั้น
            """)
            
            # Show preview
            with st.expander("👁️ Preview File Content"):
                st.json(json.loads(file_content)[:100] if len(file_content) > 100 else json.loads(file_content))
            
        except Exception as e:
            st.error(f"❌ Error during compilation: {str(e)}")
            with st.expander("🔍 Error Details"):
                import traceback
                st.code(traceback.format_exc())

else:
    st.markdown("""
    ### 📝 Instructions
    
    1. กดปุ่ม **"Compile Planner Now"** ด้านบน
    2. รอ 1-2 นาที (ระหว่างที่ compile)
    3. กด **"Download optimized_planner.json"**
    4. Commit ไฟล์นั้นเข้า repo หลัก
    5. Deploy app.py ตามปกติ
    
    ### ⚡ Why This Works
    
    - Streamlit Cloud มี compute resource พอสำหรับ compile
    - ใช้ GEMINI_API_KEY จาก secrets เหมือน app หลัก
    - Compile ครั้งเดียว ใช้ได้ตลอด
    - ไม่ต้องติดตั้งอะไรบนเครื่องตัวเอง
    
    ### 🎯 Expected Performance
    
    **ก่อน optimize (ไม่มี cached planner):**
    - Cold start: 60-120 seconds ⏳
    
    **หลัง optimize (มี cached planner):**
    - Cold start: 2-5 seconds ⚡
    - **Improvement: 95% faster!** 🚀
    """)

# Show current status
st.sidebar.header("📊 Status")
st.sidebar.write("✅ API Key configured")
st.sidebar.write(f"✅ Trainset loaded ({len(trainset)} examples)")
st.sidebar.write("⏳ Ready to compile")

st.sidebar.markdown("---")
st.sidebar.markdown("### 💡 Tips")
st.sidebar.markdown("""
- Compilation จะใช้เวลา 1-2 นาที
- ใช้ ~5-10 API calls
- ทำครั้งเดียว ใช้ได้ตลอด
- เก็บไฟล์ไว้ใน repo
""")
