"""Cached Supabase client + Claude client factories.

One place to construct the clients so every page uses the same instance.
"""
from __future__ import annotations

import os

import streamlit as st
from dotenv import load_dotenv

load_dotenv()


@st.cache_resource
def get_supabase():
    from supabase import create_client

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_KEY")
    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL / SUPABASE_KEY missing in .env — cannot connect to Supabase."
        )
    return create_client(url, key)


@st.cache_resource
def get_claude_client():
    from anthropic import Anthropic

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY missing in .env.")
    return Anthropic(api_key=api_key)


def current_user_email() -> str:
    """Best-effort identity for audit columns (applied_by, started_by)."""
    return os.getenv("USER_EMAIL") or "chef@seokitchen.nl"
