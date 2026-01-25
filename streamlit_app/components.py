"""
Reusable UI Components for Text2SQL Agent
"""

import streamlit as st
from typing import Dict, List, Any, Optional
import json


def render_metric_card(label: str, value: Any, delta: Optional[str] = None, icon: str = "📊"):
    """Render a styled metric card"""
    delta_html = f'<div style="color: #10b981; font-size: 0.8rem;">{delta}</div>' if delta else ''
    
    st.markdown(f"""
    <div style="
        background: white;
        border-radius: 12px;
        padding: 1.25rem;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        border: 1px solid #e8e8e8;
    ">
        <div style="font-size: 1.5rem; margin-bottom: 0.5rem;">{icon}</div>
        <div style="font-size: 2rem; font-weight: 700; color: #667eea;">{value}</div>
        <div style="color: #6b7280; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.5px;">{label}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


def render_status_badge(status: str, text: str):
    """Render a status badge"""
    colors = {
        "success": ("#10b981", "#d1fae5"),
        "error": ("#ef4444", "#fee2e2"),
        "warning": ("#f59e0b", "#fef3c7"),
        "info": ("#3b82f6", "#dbeafe"),
        "processing": ("#8b5cf6", "#ede9fe")
    }
    
    fg, bg = colors.get(status, colors["info"])
    
    st.markdown(f"""
    <span style="
        background: {bg};
        color: {fg};
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 600;
        display: inline-block;
    ">{text}</span>
    """, unsafe_allow_html=True)


def render_sql_block(sql: str, title: str = "Generated SQL"):
    """Render a styled SQL code block"""
    st.markdown(f"""
    <div style="margin: 1rem 0;">
        <div style="
            background: #1e1e1e;
            color: #569cd6;
            padding: 0.5rem 1rem;
            border-radius: 8px 8px 0 0;
            font-size: 0.85rem;
            font-weight: 600;
        ">{title}</div>
        <pre style="
            background: #1e1e1e;
            color: #d4d4d4;
            padding: 1rem;
            border-radius: 0 0 8px 8px;
            margin: 0;
            overflow-x: auto;
            font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
            font-size: 0.85rem;
            line-height: 1.5;
        "><code>{sql}</code></pre>
    </div>
    """, unsafe_allow_html=True)


def render_info_card(title: str, content: str, icon: str = "ℹ️"):
    """Render an info card"""
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%);
        border-left: 4px solid #0ea5e9;
        border-radius: 0 8px 8px 0;
        padding: 1rem 1.25rem;
        margin: 1rem 0;
    ">
        <div style="display: flex; align-items: center; margin-bottom: 0.5rem;">
            <span style="font-size: 1.25rem; margin-right: 0.5rem;">{icon}</span>
            <strong style="color: #0369a1;">{title}</strong>
        </div>
        <div style="color: #0c4a6e; font-size: 0.9rem;">{content}</div>
    </div>
    """, unsafe_allow_html=True)


def render_error_card(title: str, error: str):
    """Render an error card"""
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%);
        border-left: 4px solid #ef4444;
        border-radius: 0 8px 8px 0;
        padding: 1rem 1.25rem;
        margin: 1rem 0;
    ">
        <div style="display: flex; align-items: center; margin-bottom: 0.5rem;">
            <span style="font-size: 1.25rem; margin-right: 0.5rem;">❌</span>
            <strong style="color: #b91c1c;">{title}</strong>
        </div>
        <div style="color: #7f1d1d; font-size: 0.9rem;">{error}</div>
    </div>
    """, unsafe_allow_html=True)


def render_success_card(title: str, message: str):
    """Render a success card"""
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%);
        border-left: 4px solid #10b981;
        border-radius: 0 8px 8px 0;
        padding: 1rem 1.25rem;
        margin: 1rem 0;
    ">
        <div style="display: flex; align-items: center; margin-bottom: 0.5rem;">
            <span style="font-size: 1.25rem; margin-right: 0.5rem;">✅</span>
            <strong style="color: #047857;">{title}</strong>
        </div>
        <div style="color: #064e3b; font-size: 0.9rem;">{message}</div>
    </div>
    """, unsafe_allow_html=True)


def render_chat_message(role: str, content: str, metadata: Optional[Dict] = None):
    """Render a chat message bubble"""
    if role == "user":
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 1rem 1.5rem;
            border-radius: 18px 18px 4px 18px;
            margin: 0.5rem 0 0.5rem 20%;
            box-shadow: 0 2px 8px rgba(102, 126, 234, 0.3);
        ">
            <div style="font-size: 0.75rem; opacity: 0.8; margin-bottom: 0.25rem;">You</div>
            {content}
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div style="
            background: #f8f9fa;
            border: 1px solid #e8e8e8;
            color: #1f2937;
            padding: 1rem 1.5rem;
            border-radius: 18px 18px 18px 4px;
            margin: 0.5rem 20% 0.5rem 0;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        ">
            <div style="font-size: 0.75rem; color: #6b7280; margin-bottom: 0.25rem;">🤖 Assistant</div>
            {content}
        </div>
        """, unsafe_allow_html=True)


def render_progress_steps(steps: List[Dict[str, Any]], current_step: int):
    """Render a progress stepper"""
    html = '<div style="display: flex; align-items: center; justify-content: space-between; margin: 1.5rem 0;">'
    
    for i, step in enumerate(steps):
        is_complete = i < current_step
        is_current = i == current_step
        
        if is_complete:
            bg = "#10b981"
            border = "#10b981"
            color = "white"
            icon = "✓"
        elif is_current:
            bg = "#667eea"
            border = "#667eea"
            color = "white"
            icon = str(i + 1)
        else:
            bg = "white"
            border = "#d1d5db"
            color = "#9ca3af"
            icon = str(i + 1)
        
        html += f'''
        <div style="display: flex; flex-direction: column; align-items: center; flex: 1;">
            <div style="
                width: 36px;
                height: 36px;
                border-radius: 50%;
                background: {bg};
                border: 2px solid {border};
                display: flex;
                align-items: center;
                justify-content: center;
                color: {color};
                font-weight: 600;
                font-size: 0.9rem;
            ">{icon}</div>
            <div style="
                margin-top: 0.5rem;
                font-size: 0.75rem;
                color: {'#374151' if is_complete or is_current else '#9ca3af'};
                text-align: center;
            ">{step.get('label', '')}</div>
        </div>
        '''
        
        # Add connector line (except for last step)
        if i < len(steps) - 1:
            line_color = "#10b981" if is_complete else "#e5e7eb"
            html += f'''
            <div style="
                flex: 1;
                height: 2px;
                background: {line_color};
                margin: 0 0.5rem;
                margin-bottom: 1.5rem;
            "></div>
            '''
    
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


def render_table_card(table_name: str, table_info: Dict[str, Any]):
    """Render a card for a database table"""
    columns = table_info.get("columns", {})
    description = table_info.get("description", "No description available")
    domain = table_info.get("domain", "unknown")
    
    pk_count = sum(1 for c in columns.values() if c.get("pk"))
    fk_count = sum(1 for c in columns.values() if c.get("fk"))
    
    st.markdown(f"""
    <div style="
        background: white;
        border-radius: 12px;
        padding: 1.25rem;
        margin-bottom: 1rem;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
        border: 1px solid #e8e8e8;
        transition: transform 0.2s, box-shadow 0.2s;
    " onmouseover="this.style.transform='translateY(-2px)'; this.style.boxShadow='0 4px 12px rgba(0,0,0,0.1)';"
       onmouseout="this.style.transform='translateY(0)'; this.style.boxShadow='0 2px 8px rgba(0,0,0,0.06)';">
        <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 0.75rem;">
            <div>
                <h4 style="margin: 0; color: #1f2937; font-size: 1.1rem;">📋 {table_name}</h4>
                <span style="
                    background: #ede9fe;
                    color: #7c3aed;
                    padding: 0.125rem 0.5rem;
                    border-radius: 4px;
                    font-size: 0.7rem;
                    font-weight: 600;
                ">{domain}</span>
            </div>
            <div style="text-align: right;">
                <div style="font-size: 0.75rem; color: #6b7280;">
                    {len(columns)} columns
                </div>
                <div style="font-size: 0.75rem; color: #6b7280;">
                    {pk_count} PK • {fk_count} FK
                </div>
            </div>
        </div>
        <p style="margin: 0; color: #6b7280; font-size: 0.85rem; line-height: 1.4;">
            {description[:150]}{'...' if len(description) > 150 else ''}
        </p>
    </div>
    """, unsafe_allow_html=True)


def render_relationship_badge(rel: Dict[str, Any]):
    """Render a relationship badge"""
    st.markdown(f"""
    <div style="
        display: inline-flex;
        align-items: center;
        background: #f3f4f6;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        margin: 0.25rem;
        font-size: 0.85rem;
    ">
        <span style="color: #667eea; font-weight: 600;">{rel.get('from', '')}</span>
        <span style="color: #9ca3af; margin: 0 0.5rem;">•</span>
        <span style="color: #6b7280;">{rel.get('from_column', '')}</span>
        <span style="color: #9ca3af; margin: 0 0.5rem;">→</span>
        <span style="color: #764ba2; font-weight: 600;">{rel.get('to', '')}</span>
        <span style="color: #9ca3af; margin: 0 0.5rem;">•</span>
        <span style="color: #6b7280;">{rel.get('to_column', '')}</span>
    </div>
    """, unsafe_allow_html=True)