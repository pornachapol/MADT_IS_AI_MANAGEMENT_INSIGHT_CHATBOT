# app.py
import streamlit as st
import os

st.set_page_config(page_title="AI Management Insight Bot", layout="wide")

st.title("📊 AI Management Insight Chatbot")
st.caption("ถามเหมือนผู้บริหาร → แปลเป็น SQL → สรุป Insight จาก iPhone Gold Datamart")

# Check for API key
if "GEMINI_API_KEY" not in st.secrets and "GEMINI_API_KEY" not in os.environ:
    st.error("⚠️ **GEMINI_API_KEY not found!**")
    st.info("Please add your Gemini API key in Streamlit Cloud Settings → Secrets")
    st.code('GEMINI_API_KEY = "your-api-key-here"', language="toml")
    st.stop()

# Import after checking secrets
try:
    from core import ask_bot_core
except Exception as e:
    st.error(f"⚠️ **Error loading core module:**\n\n{str(e)}")
    st.info("Make sure all dependencies are installed correctly.")
    st.stop()

# Initialize session state for tracking if we've set up LM
if 'lm_initialized' not in st.session_state:
    st.session_state.lm_initialized = False

# Add a reset button in sidebar for troubleshooting
with st.sidebar:
    st.markdown("### 🔧 Troubleshooting")
    st.markdown("**⚠️ หาก error ซ้ำๆ:**")
    st.markdown("กรุณา Refresh หน้าเว็บ (กด F5)")
    st.markdown("หรือ Reboot app ใน Streamlit Cloud")
    
    st.markdown("---")
    st.markdown("**Status:**")
    st.markdown(f"- LM Initialized: {'✅' if st.session_state.lm_initialized else '⏳'}")

question = st.text_input(
    "พิมพ์คำถามผู้บริหาร",
    placeholder="เช่น เดือนนี้เราเสียโอกาสการขายไปเท่าไหร่แล้ว? หรือ เดือน 11 ปี 2025 รุ่นไหนขายดีที่สุด?",
)

if st.button("🔍 วิเคราะห์เลย", type="primary") and question.strip():
    with st.spinner("กำลังวาง SQL และสร้าง Insight..."):
        try:
            result = ask_bot_core(question)
            
            # Mark that LM has been initialized successfully
            st.session_state.lm_initialized = True

            st.subheader("🎯 Intent ที่ระบบตีความ")
            st.write(result.get("intent", "(none)"))

            st.subheader("📜 SQL ที่ใช้จริง")
            st.code(result.get("sql", ""), language="sql")

            st.subheader("📊 ผลลัพธ์ดิบจาก Datamart")
            st.markdown(result.get("table_view", ""))

            st.subheader("📌 KPI Summary")
            st.write(result.get("kpi_summary", ""))

            st.subheader("🧠 Explanation (มุมมองผู้บริหาร)")
            st.write(result.get("explanation", ""))

            st.subheader("🚀 Suggested Actions")
            st.write(result.get("action", ""))

        except AssertionError as e:
            error_msg = str(e)
            if "No LM is loaded" in error_msg or "can only be changed by the thread" in error_msg:
                # Clear cache automatically to allow retry
                st.cache_resource.clear()
                
                st.error("⚠️ **DSPy Configuration Error**")
                st.warning("🔄 **Cache cleared automatically!** กรุณา **Refresh หน้าเว็บ (F5)** แล้วลองอีกครั้ง")
                with st.expander("🔍 Technical Details"):
                    st.code(f"Error: {error_msg}")
                    st.markdown("""
                    **สาเหตุ:** LM configuration failed (อาจเกิดจาก rate limit ก่อนหน้า)
                    
                    **วิธีแก้:**
                    1. **Cache ถูก clear อัตโนมัติแล้ว**
                    2. **Refresh หน้าเว็บ (กด F5)** ← สำคัญ!
                    3. รอ 1-2 นาที ถ้าเจอ rate limit
                    4. ลองถามคำถามอีกครั้ง
                    
                    **หมายเหตุ:** ปัญหานี้มักเกิดหลังจาก API rate limit
                    """)
            else:
                raise
        
        except Exception as e:
            error_msg = str(e)
            
            # Check for rate limit errors
            if "429" in error_msg or "rate limit" in error_msg.lower() or "quota" in error_msg.lower():
                st.error("⚠️ **API Rate Limit Error**")
                st.warning("Gemini API มีการใช้งานเกินขอบเขต กรุณารอสักครู่")
                with st.expander("🔍 รายละเอียด"):
                    st.markdown(f"""
                    **Error:** {error_msg}
                    
                    **สาเหตุ:**
                    - Gemini API มี rate limit (15 requests/minute for free tier)
                    - ใช้งานเยอะเกินไปในช่วงเวลาสั้นๆ
                    
                    **วิธีแก้:**
                    1. **รอ 1-2 นาที** (จริงๆ ต้องรอ!) ⏰
                    2. Refresh หน้าเว็บ (F5) **ครั้งเดียว**
                    3. ลองถามคำถามใหม่
                    
                    **ป้องกันในอนาคต:**
                    - อย่าถามคำถามติดกันเร็วเกินไป
                    - รอให้ระบบตอบเสร็จก่อนถามใหม่ (~10 วินาที)
                    - พิจารณาอัพเกรด Gemini API tier
                    """)
            else:
                st.error(f"⚠️ **An error occurred:**\n\n{str(e)}")
                with st.expander("🔍 Debug Information"):
                    import traceback
                    st.code(traceback.format_exc())

else:
    st.info("ลองพิมพ์คำถามด้านบน แล้วกดปุ่ม 🔍 วิเคราะห์เลย")
