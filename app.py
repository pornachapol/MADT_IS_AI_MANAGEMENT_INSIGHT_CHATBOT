# app.py (patched)
import os
import traceback
import streamlit as st

st.set_page_config(page_title="AI Management Insight Bot", layout="wide")

def get_api_key():
    try:
        api_key = st.secrets.get("GEMINI_API_KEY")
    except Exception:
        api_key = None
    if not api_key:
        api_key = os.environ.get("GEMINI_API_KEY")
    return api_key

def import_core():
    try:
        from core import ask_bot_core
        return ask_bot_core
    except Exception as e:
        raise RuntimeError(f"Error loading core module: {e}")

def initialize_session():
    if "lm_initialized" not in st.session_state:
        st.session_state.lm_initialized = False
    if "history" not in st.session_state:
        st.session_state.history = []
    if "prefill" not in st.session_state:
        st.session_state.prefill = ""
    if "running" not in st.session_state:
        st.session_state.running = False  # prevent double submit

def sidebar_ui():
    with st.sidebar:
        st.title("🔧 Controls")
        st.markdown("### ตัวอย่างคำถาม")
        examples = [
            "เดือนนี้เราเสียโอกาสการขายไปเท่าไหร่?",
            "ยอดขายสินค้ากลุ่ม X เทียบเดือนก่อนเป็นอย่างไร?",
            "สัดส่วนการคืนสินค้าช่วง Q4 ของปีที่ผ่านมาเป็นอย่างไร?",
        ]
        for i, ex in enumerate(examples):
            if st.button(ex, key=f"example_{i}"):
                st.session_state.prefill = ex

        st.markdown("---")
        st.markdown("### Troubleshooting")
        st.markdown("**Status:**")
        st.markdown(f"- LM Initialized: {'✅' if st.session_state.lm_initialized else '⏳'}")
        st.markdown("---")
        st.markdown("หากเจอปัญหา: Refresh (F5) หรือ Reboot app ใน Streamlit Cloud")
        st.markdown("---")
        st.caption("เวอร์ชัน: 1.0")

def render_history():
    if st.session_state.history:
        with st.expander("ประวัติการถาม-ตอบ (History)", expanded=False):
            for i, item in enumerate(reversed(st.session_state.history[-50:])):
                q = item.get("question")
                res = item.get("result", {})
                st.markdown(f"**Q:** {q}")
                if res:
                    intent = res.get("intent", "")
                    sql = res.get("sql", "")
                    st.write(f"- Intent: {intent}")
                    if sql:
                        st.code(sql, language="sql")
                st.markdown("---")

def main():
    initialize_session()

    api_key = get_api_key()
    if not api_key:
        st.title("📊 AI Management Insight Chatbot")
        st.caption("ถามเหมือนผู้บริหาร → แปลเป็น SQL → สรุป Insight จาก iPhone Gold Datamart")
        st.error("⚠️ **GEMINI_API_KEY not found!**")
        st.info("Please add your Gemini API key in Streamlit Cloud Settings → Secrets or set the GEMINI_API_KEY environment variable")
        st.code('GEMINI_API_KEY = "your-api-key-here"', language="toml")
        st.stop()

    try:
        ask_bot_core = import_core()
    except RuntimeError as e:
        st.error("⚠️ **Error loading core module**")
        st.info(str(e))
        st.stop()

    sidebar_ui()

    st.title("📊 AI Management Insight Chatbot")
    st.caption("ถามเหมือนผู้บริหาร → แปลเป็น SQL → สรุป Insight จาก iPhone Gold Datamart")

    # Use a form so changing inputs doesn't trigger reruns immediately
    with st.form("ask_form"):
        question = st.text_input(
            "พิมพ์คำถามผู้บริหาร",
            value=st.session_state.get("prefill", ""),
            placeholder="เช่น เดือนนี้เราเสียโอกาสการขายไปเท่าไหร่? หรือ เดือน 11 ปี 2025 ยอดขายเปลี่ยนแปลงอย่างไร?"
        )
        submit = st.form_submit_button("🔍 วิเคราะห์เลย")
    # Prevent double-submit: if already running, ignore further submits
    if submit and st.session_state.running:
        st.warning("กำลังประมวลผลคำถามก่อนหน้า กรุณารอ...")
        submit = False

    if submit:
        if not question or not question.strip():
            st.error("กรุณาพิมพ์คำถามก่อนกด วิเคราะห์เลย")
        else:
            st.session_state.prefill = ""
            st.session_state.running = True
            try:
                with st.spinner("กำลังวาง SQL และสร้าง Insight..."):
                    result = ask_bot_core(question)

                    # Handle structured SQL error returned from core
                    if isinstance(result, dict) and result.get("sql_error"):
                        st.error("⚠️ เกิดข้อผิดพลาดในการรัน SQL")
                        st.warning(result.get("sql_error_message", "SQL execution failed"))
                        with st.expander("🔍 รายละเอียด SQL และคำแนะนำ"):
                            st.markdown("**SQL ที่ส่งไป:**")
                            st.code(result.get("sql", ""), language="sql")
                            available = result.get("sql_error_available_tables", [])
                            if available:
                                st.markdown("**ตารางที่มีในฐานข้อมูล (ตัวอย่าง):**")
                                for t in available:
                                    st.write(f"- {t}")
                            else:
                                st.markdown("ไม่พบตารางในฐานข้อมูล หรือไม่สามารถดึงรายการตารางได้")
                            st.markdown("---")
                            st.markdown("**แนะนำ:** ตรวจสอบชื่อตาราง/คอลัมน์ในคำถาม หรือปรับคำถามให้ใช้ตารางที่มีอยู่")
                        st.subheader("🧾 รายละเอียดจากระบบ")
                        st.write(result.get("explanation", ""))
                        st.write(result.get("action", ""))
                        # Save to history
                        st.session_state.history.append({"question": question, "result": result})
                        continue

                    # Normal successful flow
                    st.session_state.lm_initialized = True
                    st.session_state.history.append({"question": question, "result": result})

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

            except AssertionError as ae:
                error_msg = str(ae)
                if "No LM is loaded" in error_msg or "can only be changed by the thread" in error_msg:
                    try:
                        st.cache_resource.clear()
                    except Exception:
                        pass

                    st.error("⚠️ **DSPy Configuration Error**")
                    st.warning("🔄 Cache cleared automatically. กรุณา Refresh หน้าเว็บ (F5) แล้วลองอีกครั้ง")
                    with st.expander("🔍 Technical Details"):
                        st.code(f"Error: {error_msg}")
                        st.markdown("""
**สาเหตุ:** LM configuration failed (อาจเกิดจาก rate limit หรือ race condition)

**วิธีแก้:**
1. Refresh หน้าเว็บ (กด F5)
2. รอ 1-2 นาที ถ้าเป็น rate limit
3. ลองถามคำถามอีกครั้ง
""")
                else:
                    st.error(f"AssertionError: {error_msg}")
                    with st.expander("🔍 Debug"):
                        st.code(traceback.format_exc())

            except Exception as e:
                error_msg = str(e)
                if "429" in error_msg or "rate limit" in error_msg.lower() or "quota" in error_msg.lower():
                    st.error("⚠️ **API Rate Limit Error**")
                    st.warning("Gemini API อาจถูกจำกัด ขอแนะนำให้รอสักครู่ก่อนใช้งานอีกครั้ง")
                    with st.expander("🔍 รายละเอียด"):
                        st.markdown(f"**Error:** {error_msg}")
                        st.markdown("""
**แนวทางแก้ไข**
- รอสัก 1-2 นาที แล้วลองใหม่
- อย่ากดส่งซ้ำเร็วเกินไป
- พิจารณาเพิ่ม tier API ถ้าจำเป็น
""")
                else:
                    st.error("⚠️ **An unexpected error occurred**")
                    with st.expander("🔍 Debug Information"):
                        st.code(traceback.format_exc())
            finally:
                st.session_state.running = False

    else:
        st.info("ลองพิมพ์คำถามด้านบน แล้วกดปุ่ม 🔍 วิเคราะห์เลย")

    render_history()

if __name__ == "__main__":
    main()
