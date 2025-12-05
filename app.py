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

        except Exception as e:
            st.error(f"⚠️ **An error occurred:**\n\n{str(e)}")
            with st.expander("🔍 Debug Information"):
                import traceback
                st.code(traceback.format_exc())

else:
    st.info("ลองพิมพ์คำถามด้านบน แล้วกดปุ่ม 🔍 วิเคราะห์เลย")
