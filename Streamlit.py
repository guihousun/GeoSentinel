from dotenv import load_dotenv

load_dotenv(override=True)

import logging

import streamlit as st

import app_logic
import app_state
import app_ui
from storage_manager import current_thread_id

logging.basicConfig(level=logging.INFO)


def _safe_recover_runtime_health() -> bool:
    """
    Call app_logic.recover_runtime_health() if available.
    Returns True when executed, False when skipped.
    """
    recover_fn = getattr(app_logic, "recover_runtime_health", None)
    if callable(recover_fn):
        recover_fn()
        return True
    logging.warning("app_logic.recover_runtime_health is unavailable; skip runtime recovery.")
    return False


def main():
    st.set_page_config(
        page_title="地缘环境智能计算平台",
        page_icon=":robot:",
        layout="wide",
    )

    app_state.init_app()
    st.session_state["ui_lang"] = "CN"
    current_thread_id.set(st.session_state.thread_id)
    _safe_recover_runtime_health()

    app_ui.inject_css()

    top_left, _ = st.columns([0.90, 0.10])
    with top_left:
        title = "地缘环境智能计算平台"
        desc = "面向冲突事件跟踪、地缘环境监测与夜间灯光证据分析的多智能体计算平台。"
        contact_label = "联系方式"

        st.markdown(
            f"""
            <div style="border:1px solid #d7e3e0;border-radius:12px;padding:14px 16px;background:rgba(255,255,255,0.92);margin-bottom:10px;">
                <div style="font-size:1.35rem;font-weight:700;color:#123b62;">
                    {title}
                </div>
                <div style="margin-top:6px;color:#5e6b73;font-size:0.92rem;line-height:1.45;">
                    {desc}
                </div>
                <div style="margin-top:8px;color:#5e6b73;font-size:0.86rem;">
                    {contact_label}:
                    <a href="mailto:51273901095@stu.ecnu.edu.cn" style="color:#0f766e;text-decoration:none;">
                        51273901095@stu.ecnu.edu.cn
                    </a>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    app_ui.render_sidebar()
    current_thread_id.set(st.session_state.thread_id)

    if not st.session_state.get("authenticated"):
        st.markdown(
            """
            <div style="margin-top:8px;border:1px solid rgba(148,190,255,0.55);border-radius:10px;
            background:linear-gradient(180deg, rgba(14,28,58,0.88), rgba(8,16,35,0.9));
            padding:10px 12px;color:#f1f6ff;font-weight:600;">
                请先在侧边栏注册或登录，再访问你的线程历史和长期记忆。
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    app_logic.ensure_conversation_initialized()
    app_ui.render_download_center()
    app_ui.render_file_uploader()

    if not st.session_state.get("initialized"):
        st.markdown(
            """
            <div style="margin-top:8px;border:1px solid rgba(148,190,255,0.55);border-radius:10px;
            background:linear-gradient(180deg, rgba(14,28,58,0.88), rgba(8,16,35,0.9));
            padding:10px 12px;color:#f1f6ff;font-weight:600;">
                请在左侧边栏点击 <b>激活</b> 启动地缘环境智能计算平台。
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    app_ui.render_content_layout()


if __name__ == "__main__":
    main()

