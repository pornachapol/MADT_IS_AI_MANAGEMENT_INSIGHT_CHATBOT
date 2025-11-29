# app.py
import streamlit as st
from core import ask_bot_core

st.set_page_config(page_title="AI Management Insight Bot", layout="wide")

st.title("📊 AI Management Insight Chatbot")
st.caption("ถามเหมือนผู้บริหาร → แปลเป็น SQL → สรุป Insight จาก iPhone Gold Datamart")

question = st.text_input(
    "พิมพ์คำถามผู้บริหาร",
    placeholder="เช่น เดือนนี้เราเสียโอกาสการขายไปเท่าไหร่แล้ว? หรือ เดือน 11 ปี 2025 รุ่นไหนขายดีที่สุด?",
)

if st.button("🔍 วิเคราะห์เลย", type="primary") and question.strip():
    with st.spinner("กำลังวาง SQL และสร้าง Insight..."):
        result = ask_bot_core(question)

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

else:
    st.info("ลองพิมพ์คำถามด้านบน แล้วกดปุ่ม 🔍 วิเคราะห์เลย")
