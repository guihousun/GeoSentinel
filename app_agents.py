from dotenv import load_dotenv

load_dotenv(override=True)

import streamlit as st

from graph_factory import build_ntl_graph


@st.cache_resource
def get_ntl_graph(model_name: str, api_key: str, request_timeout_s: int = 120, session_tag: str = ""):
    """
    Streamlit wrapper around the pure LangGraph factory.
    Keep signature stable for UI callers.
    """
    return build_ntl_graph(
        model_name=model_name,
        api_key=api_key,
        request_timeout_s=request_timeout_s,
        graph_name="NTL_Engineer",
    )
