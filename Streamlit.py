from dotenv import load_dotenv

load_dotenv(override=True)

import logging

from ssl_compat import SSL_MODE_CERTIFI, configure_outbound_ssl

SSL_MODE = configure_outbound_ssl()

import streamlit as st

import app_logic
import app_state
import app_ui
from storage_manager import current_thread_id

logging.basicConfig(level=logging.INFO)

if SSL_MODE == SSL_MODE_CERTIFI:
    logging.warning(
        "Windows certificate store could not be parsed; using certifi for verified outbound TLS."
    )


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
        page_title="NTL-GPT: Multi-Agent Nighttime Light Analysis",
        page_icon=":robot:",
        layout="wide",
    )

    app_state.init_app()
    current_thread_id.set(st.session_state.thread_id)
    _safe_recover_runtime_health()

    app_ui.inject_css()
    app_logic.ensure_conversation_initialized()

    st.session_state.setdefault("ui_lang", "EN")
    if st.session_state.get("ui_lang") == "中文":
        st.session_state["ui_lang"] = "CN"

    top_left, top_right = st.columns([0.88, 0.12], vertical_alignment="top")
    with top_right:
        is_running = bool(st.session_state.get("is_running", False))
        lang = st.radio(
            "language",
            options=["CN", "EN"],
            horizontal=True,
            label_visibility="collapsed",
            key="ui_lang_switch",
            index=0 if st.session_state["ui_lang"] == "CN" else 1,
            disabled=is_running,
        )
        st.session_state["ui_lang"] = lang

    with top_left:
        title = "NTL-GPT: Multi-Agent Nighttime Light Remote Sensing" if st.session_state["ui_lang"] == "EN" else "NTL-GPT：夜间灯光遥感多智能体系统"
        desc = (
            "A cloud trial edition for end-to-end NTL analysis. It supports data search, preprocessing, modeling, and geospatial result generation through coordinated agents."
            if st.session_state["ui_lang"] == "EN"
            else "云端试用版，支持夜间灯光任务的端到端分析，覆盖数据检索、预处理、建模与地理空间结果生成。"
        )
        contact_label = "Contact" if st.session_state["ui_lang"] == "EN" else "联系方式"

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
    if not st.session_state.get("authenticated"):
        if st.session_state["ui_lang"] == "EN":
            st.markdown(
                """
                <div style="margin-top:8px;border:1px solid rgba(148,190,255,0.55);border-radius:10px;
                background:linear-gradient(180deg, rgba(14,28,58,0.88), rgba(8,16,35,0.9));
                padding:10px 12px;color:#f1f6ff;font-weight:600;">
                    Please register or log in from the sidebar to access your threads and memory.
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                """
                <div style="margin-top:8px;border:1px solid rgba(148,190,255,0.55);border-radius:10px;
                background:linear-gradient(180deg, rgba(14,28,58,0.88), rgba(8,16,35,0.9));
                padding:10px 12px;color:#f1f6ff;font-weight:600;">
                    请先在左侧边栏注册或登录，再访问你的线程历史和长期记忆。
                </div>
                """,
                unsafe_allow_html=True,
            )
        return

    app_ui.render_download_center()
    app_ui.render_file_uploader()

    if not st.session_state.get("initialized"):
        if st.session_state["ui_lang"] == "EN":
            st.markdown(
                """
                <div style="margin-top:8px;border:1px solid rgba(148,190,255,0.55);border-radius:10px;
                background:linear-gradient(180deg, rgba(14,28,58,0.88), rgba(8,16,35,0.9));
                padding:10px 12px;color:#f1f6ff;font-weight:600;">
                    Please click <b>Activate</b> in the sidebar to start NTL-GPT.
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                """
                <div style="margin-top:8px;border:1px solid rgba(148,190,255,0.55);border-radius:10px;
                background:linear-gradient(180deg, rgba(14,28,58,0.88), rgba(8,16,35,0.9));
                padding:10px 12px;color:#f1f6ff;font-weight:600;">
                    请在左侧边栏点击 <b>Activate</b> 启动 NTL-GPT。
                </div>
                """,
                unsafe_allow_html=True,
            )
        return

    app_ui.render_content_layout()


if __name__ == "__main__":
    main()
