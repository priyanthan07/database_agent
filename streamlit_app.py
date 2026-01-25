"""
Text2SQL Agent - Professional Chat UI
=====================================
WhatsApp-style chat interface with real-time feedback.
All backend operations go through main.py API layer.
"""

import streamlit as st
import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from uuid import UUID

# Import from main API layer only
from main import (
    setup_logging,
    get_connections,
    get_kg_connection,
    get_source_connection,
    close_connections,
    check_kg_exists,
    connect_or_build_kg,
    build_knowledge_graph,
    load_knowledge_graph,
    list_knowledge_graphs,
    get_agent_service,
    process_query,
    submit_feedback,
    get_log_file_path,
    compute_db_hash,
    ProgressUpdate,
    ConnectionResult,
    KGBuildResult,
    KGLoadResult,
    QueryResult
)

# =============================================================================
# PAGE CONFIGURATION
# =============================================================================

st.set_page_config(
    page_title="Text2SQL Agent",
    page_icon="💬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# PROFESSIONAL CSS STYLING - WhatsApp-inspired Design
# =============================================================================

st.markdown("""
<style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
    
    /* Root Variables */
    :root {
        --primary: #075E54;
        --primary-dark: #054C44;
        --secondary: #128C7E;
        --accent: #25D366;
        --user-bubble: #DCF8C6;
        --assistant-bubble: #FFFFFF;
        --chat-bg: #ECE5DD;
        --sidebar-bg: #111B21;
        --sidebar-text: #E9EDEF;
        --text-primary: #111B21;
        --text-secondary: #667781;
        --border-color: #E9EDEF;
        --error-bg: #FFEBEE;
        --error-border: #EF5350;
        --success-bg: #E8F5E9;
        --success-border: #4CAF50;
    }
    
    /* Global Styles */
    .stApp {
        font-family: 'DM Sans', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    /* Hide Streamlit Branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Sidebar Styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #111B21 0%, #1F2C33 100%);
    }
    
    [data-testid="stSidebar"] * {
        color: var(--sidebar-text) !important;
    }
    
    [data-testid="stSidebar"] .stButton > button {
        background: var(--secondary) !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        font-weight: 500 !important;
        transition: all 0.2s ease !important;
    }
    
    [data-testid="stSidebar"] .stButton > button:hover {
        background: var(--accent) !important;
        transform: translateY(-1px) !important;
    }
    
    /* Main Content Area */
    .main .block-container {
        padding: 1rem 2rem 6rem 2rem;
        max-width: 1200px;
    }
    
    /* Chat Container */
    .chat-container {
        background: var(--chat-bg);
        border-radius: 12px;
        padding: 20px;
        min-height: 500px;
        max-height: 600px;
        overflow-y: auto;
        margin-bottom: 20px;
        background-image: url("data:image/svg+xml,%3Csvg width='60' height='60' viewBox='0 0 60 60' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='none' fill-rule='evenodd'%3E%3Cg fill='%23d4cfc4' fill-opacity='0.4'%3E%3Cpath d='M36 34v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zm0-30V0h-2v4h-4v2h4v4h2V6h4V4h-4zM6 34v-4H4v4H0v2h4v4h2v-4h4v-2H6zM6 4V0H4v4H0v2h4v4h2V6h4V4H6z'/%3E%3C/g%3E%3C/g%3E%3C/svg%3E");
    }
    
    /* Message Bubbles */
    .message-row {
        display: flex;
        margin-bottom: 12px;
        animation: fadeIn 0.3s ease;
    }
    
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    .message-row.user {
        justify-content: flex-end;
    }
    
    .message-row.assistant {
        justify-content: flex-start;
    }
    
    .message-bubble {
        max-width: 75%;
        padding: 10px 14px;
        border-radius: 8px;
        position: relative;
        box-shadow: 0 1px 2px rgba(0,0,0,0.1);
    }
    
    .message-bubble.user {
        background: var(--user-bubble);
        border-top-right-radius: 0;
    }
    
    .message-bubble.assistant {
        background: var(--assistant-bubble);
        border-top-left-radius: 0;
    }
    
    .message-bubble.error {
        background: var(--error-bg);
        border-left: 3px solid var(--error-border);
    }
    
    .message-bubble.success {
        background: var(--success-bg);
        border-left: 3px solid var(--success-border);
    }
    
    .message-content {
        font-size: 14px;
        line-height: 1.5;
        color: var(--text-primary);
        word-wrap: break-word;
    }
    
    .message-time {
        font-size: 11px;
        color: var(--text-secondary);
        text-align: right;
        margin-top: 4px;
    }
    
    .message-status {
        display: inline-block;
        margin-left: 4px;
    }
    
    /* SQL Code Block */
    .sql-block {
        background: #1E1E1E;
        color: #D4D4D4;
        padding: 12px 16px;
        border-radius: 8px;
        font-family: 'JetBrains Mono', monospace;
        font-size: 13px;
        margin: 10px 0;
        overflow-x: auto;
        border-left: 3px solid var(--accent);
    }
    
    .sql-block .keyword {
        color: #569CD6;
    }
    
    .sql-block .function {
        color: #DCDCAA;
    }
    
    .sql-block .string {
        color: #CE9178;
    }
    
    /* Results Table */
    .results-table {
        background: white;
        border-radius: 8px;
        overflow: hidden;
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .results-table table {
        width: 100%;
        border-collapse: collapse;
    }
    
    .results-table th {
        background: var(--primary);
        color: white;
        padding: 10px 12px;
        text-align: left;
        font-weight: 500;
        font-size: 13px;
    }
    
    .results-table td {
        padding: 8px 12px;
        border-bottom: 1px solid var(--border-color);
        font-size: 13px;
    }
    
    .results-table tr:hover {
        background: #F5F5F5;
    }
    
    /* Header */
    .chat-header {
        background: var(--primary);
        color: white;
        padding: 16px 20px;
        border-radius: 12px 12px 0 0;
        margin: -20px -20px 20px -20px;
        display: flex;
        align-items: center;
        gap: 12px;
    }
    
    .chat-header-icon {
        width: 40px;
        height: 40px;
        background: var(--accent);
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 20px;
    }
    
    .chat-header-info h2 {
        margin: 0;
        font-size: 16px;
        font-weight: 600;
    }
    
    .chat-header-info p {
        margin: 0;
        font-size: 12px;
        opacity: 0.8;
    }
    
    /* Input Area */
    .input-container {
        background: #F0F2F5;
        padding: 12px 16px;
        border-radius: 0 0 12px 12px;
        margin: 0 -20px -20px -20px;
        display: flex;
        gap: 12px;
        align-items: center;
    }
    
    /* Status Indicator */
    .status-indicator {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 4px 10px;
        border-radius: 12px;
        font-size: 12px;
        font-weight: 500;
    }
    
    .status-indicator.connected {
        background: var(--success-bg);
        color: #2E7D32;
    }
    
    .status-indicator.disconnected {
        background: var(--error-bg);
        color: #C62828;
    }
    
    .status-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        animation: pulse 2s infinite;
    }
    
    .status-dot.connected {
        background: #4CAF50;
    }
    
    .status-dot.disconnected {
        background: #EF5350;
    }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }
    
    /* Progress Indicator */
    .progress-container {
        background: white;
        border-radius: 8px;
        padding: 16px;
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .progress-stage {
        display: flex;
        align-items: center;
        gap: 10px;
        margin-bottom: 8px;
    }
    
    .progress-icon {
        width: 24px;
        height: 24px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 12px;
    }
    
    .progress-icon.active {
        background: var(--secondary);
        color: white;
        animation: spin 1s linear infinite;
    }
    
    .progress-icon.complete {
        background: var(--accent);
        color: white;
    }
    
    .progress-icon.pending {
        background: #E0E0E0;
        color: #9E9E9E;
    }
    
    @keyframes spin {
        from { transform: rotate(0deg); }
        to { transform: rotate(360deg); }
    }
    
    /* Stat Cards */
    .stat-card {
        background: white;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        transition: transform 0.2s ease;
    }
    
    .stat-card:hover {
        transform: translateY(-2px);
    }
    
    .stat-value {
        font-size: 2.5rem;
        font-weight: 700;
        color: var(--primary);
        line-height: 1;
    }
    
    .stat-label {
        font-size: 0.85rem;
        color: var(--text-secondary);
        margin-top: 8px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    /* Form Styling */
    .stTextInput > div > div > input {
        border-radius: 8px !important;
        border: 1px solid var(--border-color) !important;
        padding: 10px 14px !important;
        font-size: 14px !important;
    }
    
    .stTextInput > div > div > input:focus {
        border-color: var(--secondary) !important;
        box-shadow: 0 0 0 2px rgba(18, 140, 126, 0.2) !important;
    }
    
    .stButton > button {
        background: var(--primary) !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 10px 24px !important;
        font-weight: 500 !important;
        transition: all 0.2s ease !important;
    }
    
    .stButton > button:hover {
        background: var(--primary-dark) !important;
        transform: translateY(-1px) !important;
    }
    
    .stButton > button[kind="primary"] {
        background: var(--accent) !important;
    }
    
    /* Typing Indicator */
    .typing-indicator {
        display: flex;
        gap: 4px;
        padding: 10px 14px;
        background: var(--assistant-bubble);
        border-radius: 8px;
        width: fit-content;
    }
    
    .typing-dot {
        width: 8px;
        height: 8px;
        background: var(--text-secondary);
        border-radius: 50%;
        animation: typing 1.4s infinite;
    }
    
    .typing-dot:nth-child(2) {
        animation-delay: 0.2s;
    }
    
    .typing-dot:nth-child(3) {
        animation-delay: 0.4s;
    }
    
    @keyframes typing {
        0%, 60%, 100% { transform: translateY(0); }
        30% { transform: translateY(-4px); }
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: transparent;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: white;
        border-radius: 8px;
        padding: 8px 16px;
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        background: var(--primary) !important;
        color: white !important;
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        background: #F5F5F5;
        border-radius: 8px;
        font-weight: 500;
    }
    
    /* Scrollbar */
    .chat-container::-webkit-scrollbar {
        width: 6px;
    }
    
    .chat-container::-webkit-scrollbar-track {
        background: transparent;
    }
    
    .chat-container::-webkit-scrollbar-thumb {
        background: rgba(0,0,0,0.2);
        border-radius: 3px;
    }
    
    /* Logo */
    .logo-container {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 16px 0;
        border-bottom: 1px solid rgba(255,255,255,0.1);
        margin-bottom: 20px;
    }
    
    .logo-icon {
        width: 48px;
        height: 48px;
        background: linear-gradient(135deg, var(--secondary) 0%, var(--accent) 100%);
        border-radius: 12px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 24px;
    }
    
    .logo-text {
        font-size: 20px;
        font-weight: 700;
        letter-spacing: -0.5px;
    }
    
    .logo-subtext {
        font-size: 11px;
        opacity: 0.7;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# SESSION STATE INITIALIZATION
# =============================================================================

def init_session_state():
    """Initialize all session state variables"""
    defaults = {
        "messages": [],
        "kg_loaded": False,
        "kg_info": {},
        "kg_uuid": None,
        "kg_data": None,
        "agent_service": None,
        "source_conn": None,
        "kg_conn": None,
        "settings": None,
        "processing": False,
        "current_stage": None,
        "source_credentials": None  # Store source DB credentials for reconnection
    }
    
    for key, default in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default


init_session_state()


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def format_timestamp(dt: Optional[datetime] = None) -> str:
    """Format timestamp for display"""
    if dt is None:
        dt = datetime.now()
    return dt.strftime("%I:%M %p")


def render_message(role: str, content: str, timestamp: str = None, 
                   sql: str = None, results: List[Dict] = None, 
                   error: bool = False, metadata: Dict = None):
    """Render a chat message bubble"""
    
    timestamp = timestamp or format_timestamp()
    bubble_class = "error" if error else role
    
    if role == "user":
        st.markdown(f"""
        <div class="message-row user">
            <div class="message-bubble user">
                <div class="message-content">{content}</div>
                <div class="message-time">{timestamp} <span class="message-status">✓✓</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        status_icon = "❌" if error else "✓"
        st.markdown(f"""
        <div class="message-row assistant">
            <div class="message-bubble {bubble_class}">
                <div class="message-content">{content}</div>
                <div class="message-time">{timestamp}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Show SQL if available
        if sql:
            with st.expander("📝 Generated SQL", expanded=False):
                st.code(sql, language="sql")
        
        # Show results if available
        if results and len(results) > 0:
            with st.expander(f"📊 Results ({len(results)} rows)", expanded=True):
                df = pd.DataFrame(results)
                st.dataframe(df, width="stretch", hide_index=True)
        
        # Show metadata if available
        if metadata and not error:
            with st.expander("ℹ️ Query Details", expanded=False):
                cols = st.columns(4)
                timing = metadata.get("timing", {})
                cols[0].metric("Tables Used", len(metadata.get("tables_used", [])))
                cols[1].metric("Confidence", f"{metadata.get('confidence_score', 0):.0%}")
                cols[2].metric("Iterations", metadata.get("iterations", 1))
                cols[3].metric("Total Time", f"{timing.get('total_ms', 0)}ms")


def render_typing_indicator():
    """Render typing indicator"""
    st.markdown("""
    <div class="message-row assistant">
        <div class="typing-indicator">
            <div class="typing-dot"></div>
            <div class="typing-dot"></div>
            <div class="typing-dot"></div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_progress_stages(current_stage: str, message: str = ""):
    """Render progress stages with detailed feedback"""
    stages = [
        ("initialization", "Initializing", "⚙️"),
        ("query_analysis", "Analyzing Query", "🔍"),
        ("schema_selection", "Selecting Tables", "📋"),
        ("sql_generation", "Generating SQL", "⚡"),
        ("execution", "Executing Query", "🚀"),
        ("complete", "Complete", "✅")
    ]
    
    stage_order = [s[0] for s in stages]
    current_idx = stage_order.index(current_stage) if current_stage in stage_order else -1
    
    html = '''
    <div class="progress-container" style="background: white; border-radius: 12px; padding: 20px; margin: 15px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
        <div style="display: flex; align-items: center; margin-bottom: 16px;">
            <div style="width: 40px; height: 40px; background: linear-gradient(135deg, #128C7E 0%, #25D366 100%); border-radius: 50%; display: flex; align-items: center; justify-content: center; margin-right: 12px;">
                <span style="font-size: 20px;">🤖</span>
            </div>
            <div>
                <div style="font-weight: 600; color: #111B21; font-size: 14px;">Processing your query</div>
                <div style="font-size: 12px; color: #667781;">''' + (message or "Please wait...") + '''</div>
            </div>
        </div>
        <div style="display: flex; justify-content: space-between; position: relative; padding: 0 10px;">
            <div style="position: absolute; top: 12px; left: 30px; right: 30px; height: 2px; background: #E0E0E0; z-index: 0;"></div>
    '''
    
    for idx, (stage_id, label, icon) in enumerate(stages):
        if idx < current_idx:
            bg_color = "#25D366"
            text_color = "#2E7D32"
            display_icon = "✓"
            font_weight = "500"
        elif idx == current_idx:
            bg_color = "#128C7E"
            text_color = "#075E54"
            display_icon = "⟳"
            font_weight = "600"
        else:
            bg_color = "#E0E0E0"
            text_color = "#9E9E9E"
            display_icon = str(idx + 1)
            font_weight = "400"
        
        animation = "animation: spin 1s linear infinite;" if idx == current_idx else ""
        
        html += f'''
            <div style="display: flex; flex-direction: column; align-items: center; z-index: 1;">
                <div style="width: 28px; height: 28px; background: {bg_color}; border-radius: 50%; display: flex; align-items: center; justify-content: center; color: white; font-size: 12px; font-weight: 600; {animation}">
                    {display_icon}
                </div>
                <div style="font-size: 10px; color: {text_color}; margin-top: 6px; font-weight: {font_weight}; text-align: center; max-width: 60px;">
                    {label}
                </div>
            </div>
        '''
    
    html += '''
        </div>
    </div>
    <style>
        @keyframes spin {
            from { transform: rotate(0deg); }
            to { transform: rotate(360deg); }
        }
    </style>
    '''
    
    st.markdown(html, unsafe_allow_html=True)


def render_agent_feedback(stage: str, details: Dict[str, Any] = None):
    """Render detailed agent feedback panel"""
    
    stage_info = {
        "initialization": {
            "title": "Initializing Agent",
            "icon": "⚙️",
            "description": "Setting up the query processing pipeline...",
            "color": "#607D8B"
        },
        "query_analysis": {
            "title": "Agent 1: Query Analysis",
            "icon": "🔍",
            "description": "Understanding your natural language query and detecting any ambiguities...",
            "color": "#2196F3"
        },
        "schema_selection": {
            "title": "Agent 1: Schema Selection",
            "icon": "📋",
            "description": "Identifying relevant tables and columns using vector search and graph traversal...",
            "color": "#9C27B0"
        },
        "sql_generation": {
            "title": "Agent 2: SQL Generation",
            "icon": "⚡",
            "description": "Generating optimized SQL query based on selected schema...",
            "color": "#FF9800"
        },
        "execution": {
            "title": "Agent 3: Execution & Validation",
            "icon": "🚀",
            "description": "Executing query and validating results...",
            "color": "#4CAF50"
        },
        "complete": {
            "title": "Complete",
            "icon": "✅",
            "description": "Query processed successfully!",
            "color": "#25D366"
        },
        "error": {
            "title": "Error",
            "icon": "❌",
            "description": "An error occurred during processing.",
            "color": "#F44336"
        }
    }
    
    info = stage_info.get(stage, stage_info["initialization"])
    
    details_html = ""
    if details:
        details_html = "<div style='margin-top: 12px; padding-top: 12px; border-top: 1px solid #E0E0E0;'>"
        for key, value in details.items():
            details_html += f"<div style='font-size: 12px; color: #667781; margin-bottom: 4px;'><strong>{key}:</strong> {value}</div>"
        details_html += "</div>"
    
    html = f'''
    <div style="background: white; border-radius: 12px; padding: 16px; margin: 10px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.08); border-left: 4px solid {info['color']};">
        <div style="display: flex; align-items: center; gap: 12px;">
            <div style="width: 36px; height: 36px; background: {info['color']}20; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 18px;">
                {info['icon']}
            </div>
            <div style="flex: 1;">
                <div style="font-weight: 600; color: #111B21; font-size: 14px;">{info['title']}</div>
                <div style="font-size: 12px; color: #667781; margin-top: 2px;">{info['description']}</div>
            </div>
            <div style="width: 20px; height: 20px;">
                <svg viewBox="0 0 24 24" style="animation: spin 1s linear infinite;">
                    <circle cx="12" cy="12" r="10" stroke="{info['color']}" stroke-width="3" fill="none" stroke-dasharray="31.4 31.4" stroke-linecap="round"/>
                </svg>
            </div>
        </div>
        {details_html}
    </div>
    '''
    
    st.markdown(html, unsafe_allow_html=True)


# =============================================================================
# SIDEBAR
# =============================================================================

def render_sidebar():
    """Render sidebar with navigation and status"""
    
    with st.sidebar:
        # Logo
        st.markdown("""
        <div class="logo-container">
            <div class="logo-icon">💬</div>
            <div>
                <div class="logo-text">Text2SQL</div>
                <div class="logo-subtext">AI Agent</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Navigation
        page = st.radio(
            "Navigation",
            ["💬 Chat", "🔨 Build KG", "🔍 View KG"],
            label_visibility="collapsed"
        )
        
        st.divider()
        
        # Connection Status
        if st.session_state.kg_loaded:
            st.markdown(f"""
            <div class="status-indicator connected">
                <div class="status-dot connected"></div>
                Connected
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown(f"**Database:** {st.session_state.kg_info.get('db_name', 'Unknown')}")
            st.markdown(f"**Tables:** {st.session_state.kg_info.get('tables', 0)}")
            st.markdown(f"**Relationships:** {st.session_state.kg_info.get('relationships', 0)}")
        else:
            st.markdown("""
            <div class="status-indicator disconnected">
                <div class="status-dot disconnected"></div>
                Not Connected
            </div>
            """, unsafe_allow_html=True)
            st.caption("Build or load a Knowledge Graph to start")
        
        st.divider()
        
        # Quick Actions
        st.markdown("**Quick Actions**")
        
        if st.button("🗑️ Clear Chat", use_container_width=True):
            st.session_state.messages = []
            st.rerun()
        
        if st.session_state.kg_loaded:
            if st.button("🔄 Reload KG", use_container_width=True):
                reload_kg()
        
        st.divider()
        
        # Log file info
        st.caption(f"📄 Logs: `{get_log_file_path()}`")
        
        return page


def reload_kg():
    """Reload the current Knowledge Graph"""
    if st.session_state.kg_uuid and st.session_state.kg_conn:
        result = load_knowledge_graph(
            kg_conn=st.session_state.kg_conn,
            settings=st.session_state.settings,
            kg_id=st.session_state.kg_uuid
        )
        if result.success:
            st.session_state.kg_data = result.kg_data
            st.success("Knowledge Graph reloaded!")
        else:
            st.error(f"Failed to reload: {result.error}")


# =============================================================================
# CHAT PAGE
# =============================================================================

def chat_page():
    """Main chat interface"""
    
    # Header
    st.markdown("""
    <div class="chat-header">
        <div class="chat-header-icon">🤖</div>
        <div class="chat-header-info">
            <h2>SQL Assistant</h2>
            <p>Ask questions about your database in natural language</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    if not st.session_state.kg_loaded:
        st.warning("⚠️ Please build or load a Knowledge Graph first to start chatting.")
        st.info("Go to **Build KG** in the sidebar to connect to your database.")
        return
    
    # Chat container
    chat_container = st.container()
    
    with chat_container:
        st.markdown('<div class="chat-container">', unsafe_allow_html=True)
        
        # Welcome message if no messages
        if not st.session_state.messages:
            st.markdown("""
            <div class="message-row assistant">
                <div class="message-bubble assistant">
                    <div class="message-content">
                        👋 Hello! I'm your SQL Assistant. Ask me anything about your database and I'll generate the SQL query for you.
                        <br><br>
                        <strong>Try asking:</strong><br>
                        • "Show me all customers"<br>
                        • "What are the top 5 products by revenue?"<br>
                        • "Find orders from last month"
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        # Render messages
        for msg in st.session_state.messages:
            render_message(
                role=msg["role"],
                content=msg["content"],
                timestamp=msg.get("timestamp"),
                sql=msg.get("sql"),
                results=msg.get("results"),
                error=msg.get("error", False),
                metadata=msg.get("metadata")
            )
        
        # Show processing indicator if processing
        if st.session_state.processing:
            render_typing_indicator()
            if st.session_state.current_stage:
                render_progress_stages(st.session_state.current_stage, "Processing your query...")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Input area
    st.markdown("---")
    
    col1, col2 = st.columns([6, 1])
    
    with col1:
        user_input = st.text_input(
            "Message",
            placeholder="Ask a question about your database...",
            key="chat_input",
            label_visibility="collapsed"
        )
    
    with col2:
        send_button = st.button("Send", type="primary", use_container_width=True)
    
    # Handle send
    if (send_button or user_input) and user_input and not st.session_state.processing:
        handle_user_message(user_input)


def handle_user_message(user_input: str):
    """Handle user message submission"""
    
    # Add user message
    st.session_state.messages.append({
        "role": "user",
        "content": user_input,
        "timestamp": format_timestamp()
    })
    
    # Set processing state
    st.session_state.processing = True
    st.session_state.current_stage = "initialization"
    st.rerun()


def process_pending_query():
    """Process any pending query (called after rerun)"""
    
    if not st.session_state.processing:
        return
    
    if not st.session_state.messages:
        st.session_state.processing = False
        return
    
    # Get last user message
    last_msg = st.session_state.messages[-1]
    if last_msg["role"] != "user":
        st.session_state.processing = False
        return
    
    user_query = last_msg["content"]
    
    # Create a placeholder for progress updates
    progress_placeholder = st.empty()
    
    def update_ui_progress(update: ProgressUpdate):
        """Update UI with progress"""
        st.session_state.current_stage = update.stage
        with progress_placeholder.container():
            render_progress_stages(update.stage, update.message)
            if update.details:
                render_agent_feedback(update.stage, update.details)
    
    # Process query
    try:
        # Show initial progress
        update_ui_progress(ProgressUpdate(
            stage="initialization",
            message="Starting query processing...",
            progress=0.0
        ))
        
        result = process_query(
            agent_service=st.session_state.agent_service,
            kg_id=st.session_state.kg_uuid,
            user_query=user_query,
            progress_callback=update_ui_progress
        )
        
        # Clear progress
        progress_placeholder.empty()
        
        if result.needs_clarification:
            # Handle clarification request
            clarification = result.clarification_request
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"🤔 **I need some clarification:**\n\n{clarification.get('question', 'Please clarify your question.')}\n\n**Options:**\n" + 
                          "\n".join([f"• {opt}" for opt in clarification.get('options', [])]),
                "timestamp": format_timestamp()
            })
        elif result.success:
            row_count = len(result.data) if result.data else 0
            
            # Build success message with timing info
            timing = result.metadata.get("timing", {})
            timing_info = f" in {timing.get('total_ms', 0)}ms" if timing.get('total_ms') else ""
            
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"✅ Found **{row_count}** results{timing_info}",
                "timestamp": format_timestamp(),
                "sql": result.sql,
                "results": result.data,
                "metadata": result.metadata
            })
        else:
            error_msg = result.error or 'Unknown error occurred'
            category = f" ({result.error_category})" if result.error_category else ""
            
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"❌ **Error{category}:** {error_msg}",
                "timestamp": format_timestamp(),
                "error": True,
                "sql": result.sql,
                "metadata": result.metadata
            })
    
    except Exception as e:
        progress_placeholder.empty()
        st.session_state.messages.append({
            "role": "assistant",
            "content": f"❌ **Error:** {str(e)}",
            "timestamp": format_timestamp(),
            "error": True
        })
    
    finally:
        st.session_state.processing = False
        st.session_state.current_stage = None
        st.rerun()


# =============================================================================
# KG BUILDER PAGE
# =============================================================================

def kg_builder_page():
    """Knowledge Graph builder interface"""
    
    st.markdown("## 🔨 Knowledge Graph Builder")
    st.caption("Connect to your database and build a Knowledge Graph for natural language queries.")
    
    # Show current KG stats if loaded
    if st.session_state.kg_loaded:
        st.success(f"✅ Currently connected to: **{st.session_state.kg_info.get('db_name')}**")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value">{st.session_state.kg_info.get('tables', 0)}</div>
                <div class="stat-label">Tables</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value">{st.session_state.kg_info.get('relationships', 0)}</div>
                <div class="stat-label">Relationships</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value">{st.session_state.kg_info.get('columns', 0)}</div>
                <div class="stat-label">Columns</div>
            </div>
            """, unsafe_allow_html=True)
        
        st.divider()
    
    # Connection form
    st.markdown("### Database Connection")
    
    with st.form("kg_builder_form"):
        col1, col2 = st.columns([3, 1])
        
        with col1:
            host = st.text_input("Host", value="localhost", placeholder="localhost")
        with col2:
            port = st.number_input("Port", value=5432, min_value=1, max_value=65535)
        
        db_name = st.text_input("Database Name", value="ecommerce_db", placeholder="your_database")
        
        col1, col2 = st.columns(2)
        with col1:
            user = st.text_input("Username", value="postgres", placeholder="postgres")
        with col2:
            password = st.text_input("Password", type="password", placeholder="Enter password")
        
        st.divider()
        
        col1, col2 = st.columns(2)
        with col1:
            generate_descriptions = st.checkbox("Generate AI Descriptions", value=True)
        with col2:
            generate_embeddings = st.checkbox("Generate Embeddings", value=True)
        
        submitted = st.form_submit_button("🔨 Build Knowledge Graph", type="primary", use_container_width=True)
    
    if submitted:
        if not db_name:
            st.error("Please enter a database name")
            return
        
        build_kg_with_progress(host, port, db_name, user, password, generate_descriptions, generate_embeddings)


def build_kg_with_progress(host: str, port: int, db_name: str, user: str, password: str,
                           generate_descriptions: bool, generate_embeddings: bool):
    """
    Connect to database and build/load KG with detailed progress feedback.
    
    Source database credentials come from user input.
    KG storage credentials come from settings/environment.
    If KG already exists for this database, it will be loaded instead of rebuilt.
    """
    
    # Progress container
    progress_container = st.container()
    
    with progress_container:
        progress_bar = st.progress(0)
        status_container = st.empty()
        details_container = st.empty()
    
    def update_progress(update: ProgressUpdate):
        progress_bar.progress(min(update.progress, 1.0))
        
        # Stage-specific styling
        stage_colors = {
            "initialization": "#607D8B",
            "loading": "#2196F3",
            "building": "#9C27B0",
            "schema_extraction": "#2196F3",
            "description_generation": "#9C27B0",
            "embedding_generation": "#FF9800",
            "storage": "#4CAF50",
            "complete": "#25D366",
            "error": "#F44336"
        }
        
        color = stage_colors.get(update.stage, "#128C7E")
        
        with status_container:
            st.markdown(f"""
            <div style="background: white; border-radius: 12px; padding: 16px; margin: 10px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.08); border-left: 4px solid {color};">
                <div style="display: flex; align-items: center; gap: 12px;">
                    <div style="font-weight: 600; color: #111B21; font-size: 14px;">
                        {update.stage.replace('_', ' ').title()}
                    </div>
                </div>
                <div style="font-size: 13px; color: #667781; margin-top: 8px;">
                    {update.message}
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        if update.details:
            with details_container:
                detail_items = " | ".join([f"<strong>{k}:</strong> {v}" for k, v in update.details.items()])
                st.markdown(f"""
                <div style="background: #F5F5F5; border-radius: 8px; padding: 10px 14px; font-size: 12px; color: #667781;">
                    {detail_items}
                </div>
                """, unsafe_allow_html=True)
    
    # Use connect_or_build_kg which handles:
    # 1. Connect to KG storage (from settings)
    # 2. Connect to source database (from user input)
    # 3. Check if KG exists -> load it, or build new
    result = connect_or_build_kg(
        source_host=host,
        source_port=port,
        source_db=db_name,
        source_user=user,
        source_password=password,
        generate_descriptions=generate_descriptions,
        generate_embeddings=generate_embeddings,
        progress_callback=update_progress
    )
    
    # Clear progress UI
    progress_bar.empty()
    status_container.empty()
    details_container.empty()
    
    if result.success:
        # Get fresh connections for session state
        conn_result = get_connections(
            source_host=host,
            source_port=port,
            source_db=db_name,
            source_user=user,
            source_password=password
        )
        
        if not conn_result.success:
            st.error(f"❌ Failed to establish connections: {conn_result.error}")
            return
        
        # Store in session state
        st.session_state.source_conn = conn_result.source_conn
        st.session_state.kg_conn = conn_result.kg_conn
        st.session_state.settings = conn_result.settings
        st.session_state.kg_uuid = result.kg_id
        st.session_state.kg_loaded = True
        st.session_state.kg_data = result.kg_data
        st.session_state.kg_info = {
            "db_name": db_name,
            "host": host,
            "port": port,
            "user": user,
            "tables": result.tables_count,
            "relationships": result.relationships_count,
            "columns": result.columns_count
        }
        
        # Store source credentials for reconnection
        st.session_state.source_credentials = {
            "host": host,
            "port": port,
            "database": db_name,
            "user": user,
            "password": password
        }
        
        # Initialize agent service
        st.session_state.agent_service = get_agent_service(
            kg_conn=conn_result.kg_conn,
            source_conn=conn_result.source_conn,
            settings=conn_result.settings
        )
        
        # Success message with animation
        st.markdown("""
        <div style="background: linear-gradient(135deg, #E8F5E9 0%, #C8E6C9 100%); border-radius: 12px; padding: 24px; text-align: center; margin: 20px 0;">
            <div style="font-size: 48px; margin-bottom: 12px;">🎉</div>
            <div style="font-size: 20px; font-weight: 600; color: #2E7D32; margin-bottom: 8px;">Knowledge Graph Built Successfully!</div>
            <div style="font-size: 14px; color: #4CAF50;">Your database is now ready for natural language queries</div>
        </div>
        """, unsafe_allow_html=True)
        
        st.balloons()
        
        # Show summary cards
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value">{result.tables_count}</div>
                <div class="stat-label">Tables</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value">{result.relationships_count}</div>
                <div class="stat-label">Relationships</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value">{result.columns_count}</div>
                <div class="stat-label">Columns</div>
            </div>
            """, unsafe_allow_html=True)
        
        st.info("💡 **Tip:** Go to the **Chat** page to start asking questions about your database!")
        
    else:
        st.error(f"❌ Build failed: {result.error}")
        
        # Close connections on failure
        close_connections(conn_result.source_conn, conn_result.kg_conn)


# =============================================================================
# KG VIEWER PAGE
# =============================================================================

def kg_viewer_page():
    """Knowledge Graph viewer interface"""
    
    st.markdown("## 🔍 Knowledge Graph Viewer")
    
    if not st.session_state.kg_loaded:
        st.info("📊 Build or load a Knowledge Graph first to view its structure.")
        return
    
    # Stats
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-value">{st.session_state.kg_info.get('tables', 0)}</div>
            <div class="stat-label">Tables</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-value">{st.session_state.kg_info.get('relationships', 0)}</div>
            <div class="stat-label">Relationships</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-value">{st.session_state.kg_info.get('columns', 0)}</div>
            <div class="stat-label">Columns</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["📊 Graph View", "📋 Table Browser", "📄 JSON Export"])
    
    with tab1:
        render_graph_view()
    
    with tab2:
        render_table_browser()
    
    with tab3:
        render_json_export()


def render_graph_view():
    """Render graph visualization"""
    
    kg_data = st.session_state.kg_data
    if not kg_data:
        st.warning("No graph data available")
        return
    
    try:
        import plotly.graph_objects as go
        import networkx as nx
        
        G = nx.DiGraph()
        
        # Add nodes
        for table_name, info in kg_data["tables"].items():
            G.add_node(table_name, cols=len(info["columns"]))
        
        # Add edges
        for rel in kg_data["relationships"]:
            G.add_edge(rel["from"], rel["to"])
        
        # Layout
        pos = nx.spring_layout(G, k=2, iterations=50, seed=42)
        
        # Create edge traces
        edge_x, edge_y = [], []
        for edge in G.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
        
        edge_trace = go.Scatter(
            x=edge_x, y=edge_y,
            line=dict(width=2, color='#128C7E'),
            hoverinfo='none',
            mode='lines'
        )
        
        # Create node traces
        node_x, node_y, node_text, node_hover = [], [], [], []
        for node in G.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            node_text.append(node)
            cols = G.nodes[node]['cols']
            node_hover.append(f"<b>{node}</b><br>{cols} columns")
        
        node_trace = go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text',
            text=node_text,
            textposition="top center",
            textfont=dict(size=12, color='#111B21'),
            hovertext=node_hover,
            hoverinfo='text',
            marker=dict(
                size=30,
                color='#075E54',
                line=dict(width=3, color='white'),
                symbol='circle'
            )
        )
        
        # Create figure
        fig = go.Figure(
            data=[edge_trace, node_trace],
            layout=go.Layout(
                showlegend=False,
                hovermode='closest',
                margin=dict(b=20, l=20, r=20, t=20),
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                height=500,
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)'
            )
        )
        
        st.plotly_chart(fig, use_container_width=True, key="kg_graph")
        
    except ImportError:
        st.warning("Install plotly and networkx for graph visualization: `pip install plotly networkx`")
        
        # Fallback: show relationships as list
        st.markdown("**Relationships:**")
        for rel in kg_data["relationships"]:
            st.markdown(f"• `{rel['from']}` → `{rel['to']}`")


def render_table_browser():
    """Render table browser"""
    
    kg_data = st.session_state.kg_data
    if not kg_data:
        st.warning("No table data available")
        return
    
    tables = list(kg_data["tables"].keys())
    selected_table = st.selectbox("Select a table", tables)
    
    if selected_table:
        table_info = kg_data["tables"][selected_table]
        
        st.markdown(f"### {selected_table}")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Description:** {table_info.get('description', 'N/A')}")
        with col2:
            st.markdown(f"**Domain:** {table_info.get('domain', 'N/A')}")
        
        st.markdown("#### Columns")
        
        columns = table_info.get("columns", {})
        if columns:
            col_data = []
            for col_name, col_info in columns.items():
                col_data.append({
                    "Column": col_name,
                    "Type": col_info.get("type", "unknown"),
                    "PK": "✓" if col_info.get("pk") else "",
                    "FK": "✓" if col_info.get("fk") else "",
                    "Description": col_info.get("description", "")[:50] + "..." if col_info.get("description") and len(col_info.get("description", "")) > 50 else col_info.get("description", "")
                })
            
            df = pd.DataFrame(col_data)
            st.dataframe(df, width="stretch", hide_index=True)


def render_json_export():
    """Render JSON export"""
    
    kg_data = st.session_state.kg_data
    if not kg_data:
        st.warning("No data available for export")
        return
    
    json_str = json.dumps(kg_data, indent=2, default=str)
    
    st.code(json_str, language="json")
    
    st.download_button(
        label="📥 Download JSON",
        data=json_str,
        file_name=f"kg_{st.session_state.kg_info.get('db_name', 'export')}.json",
        mime="application/json"
    )


# =============================================================================
# MAIN APPLICATION
# =============================================================================

def main():
    """Main application entry point"""
    
    # Process any pending query
    if st.session_state.processing:
        process_pending_query()
    
    # Render sidebar and get selected page
    page = render_sidebar()
    
    # Route to selected page
    if page == "💬 Chat":
        chat_page()
    elif page == "🔨 Build KG":
        kg_builder_page()
    elif page == "🔍 View KG":
        kg_viewer_page()


if __name__ == "__main__":
    main()
