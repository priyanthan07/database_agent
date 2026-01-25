"""
Text2SQL Agent - Premium Streamlit UI
Production-grade interface with WhatsApp-style chat, real-time logs, 
Knowledge Graph visualization, and animated agent workflow.
"""

import streamlit as st
import sys
import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from uuid import UUID
import pandas as pd

# Add parent directory to path
sys.path.insert(0, os.path.dirname(__file__))

# ============================================================================
# CUSTOM LOG HANDLER - Captures all backend logs for UI display
# ============================================================================

class StreamlitLogHandler(logging.Handler):
    """Custom log handler that captures logs into Streamlit session state"""
    
    def __init__(self):
        super().__init__()
        self.setFormatter(logging.Formatter('%(name)s - %(message)s'))
    
    def emit(self, record):
        try:
            if "logs" not in st.session_state:
                st.session_state.logs = []
            
            log_entry = {
                "time": datetime.now().strftime("%H:%M:%S.%f")[:-3],
                "level": record.levelname,
                "logger": record.name.split('.')[-1][:15],  # Short logger name
                "msg": self.format(record)
            }
            st.session_state.logs.append(log_entry)
            
            # Keep only last 500 logs
            if len(st.session_state.logs) > 500:
                st.session_state.logs = st.session_state.logs[-500:]
        except Exception:
            pass  # Don't let logging errors break the app


def setup_logging():
    """Setup logging to capture all backend logs"""
    if "logging_setup" not in st.session_state:
        handler = StreamlitLogHandler()
        handler.setLevel(logging.DEBUG)
        
        # Add handler to root logger and specific loggers
        loggers_to_capture = [
            'src.agents',
            'src.kg',
            'src.api',
            'src.orchestration',
            'src.memory',
            '__main__',
            'root'
        ]
        
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.DEBUG)
        
        for logger_name in loggers_to_capture:
            logger = logging.getLogger(logger_name)
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
        
        st.session_state.logging_setup = True


# ============================================================================
# PAGE CONFIG & CSS
# ============================================================================

st.set_page_config(
    page_title="Text2SQL Agent",
    page_icon="🗃️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Premium WhatsApp-style CSS
CUSTOM_CSS = """
<style>
    /* Main theme */
    .stApp {
        background: linear-gradient(135deg, #0b141a 0%, #111b21 100%);
    }
    
    .stSidebar {
        background-color: #111b21 !important;
    }
    
    .stSidebar [data-testid="stSidebarContent"] {
        background-color: #111b21 !important;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Chat container */
    .chat-container {
        max-height: 500px;
        overflow-y: auto;
        padding: 16px;
        background: #0b141a;
        border-radius: 12px;
    }
    
    /* User message - RIGHT aligned (WhatsApp style) */
    .user-bubble {
        background: linear-gradient(135deg, #005c4b 0%, #004d40 100%);
        color: #e9edef;
        padding: 12px 16px;
        border-radius: 12px 12px 0 12px;
        margin: 8px 0 8px 25%;
        max-width: 75%;
        box-shadow: 0 2px 8px rgba(0,0,0,0.3);
        position: relative;
    }
    
    .user-bubble::after {
        content: '';
        position: absolute;
        right: -8px;
        bottom: 0;
        border-width: 8px;
        border-style: solid;
        border-color: transparent transparent #004d40 transparent;
    }
    
    /* Assistant message - LEFT aligned */
    .assistant-bubble {
        background: linear-gradient(135deg, #202c33 0%, #1a252b 100%);
        color: #e9edef;
        padding: 12px 16px;
        border-radius: 12px 12px 12px 0;
        margin: 8px 25% 8px 0;
        max-width: 75%;
        box-shadow: 0 2px 8px rgba(0,0,0,0.3);
        position: relative;
    }
    
    .assistant-bubble::after {
        content: '';
        position: absolute;
        left: -8px;
        bottom: 0;
        border-width: 8px;
        border-style: solid;
        border-color: transparent #1a252b transparent transparent;
    }
    
    /* Clarification MCQ bubble */
    .clarification-bubble {
        background: linear-gradient(135deg, #202c33 0%, #1a252b 100%);
        border: 2px solid #f59e0b;
        color: #e9edef;
        padding: 16px;
        border-radius: 12px;
        margin: 8px 20% 8px 0;
        max-width: 80%;
        box-shadow: 0 2px 12px rgba(245, 158, 11, 0.2);
    }
    
    .clarification-title {
        color: #f59e0b;
        font-weight: 600;
        font-size: 0.9rem;
        margin-bottom: 8px;
        display: flex;
        align-items: center;
        gap: 8px;
    }
    
    /* Results bubble */
    .results-bubble {
        background: linear-gradient(135deg, #1e3a2f 0%, #1a2f26 100%);
        border: 1px solid #00a884;
        color: #e9edef;
        padding: 16px;
        border-radius: 12px;
        margin: 8px 15% 8px 0;
        max-width: 85%;
    }
    
    /* Error bubble */
    .error-bubble {
        background: linear-gradient(135deg, #3a1e1e 0%, #2f1a1a 100%);
        border: 1px solid #ef4444;
        color: #e9edef;
        padding: 16px;
        border-radius: 12px;
        margin: 8px 15% 8px 0;
        max-width: 85%;
    }
    
    /* Timestamp */
    .msg-time {
        font-size: 0.7rem;
        color: rgba(255,255,255,0.5);
        text-align: right;
        margin-top: 4px;
    }
    
    /* Logs panel */
    .logs-container {
        background: #0d1418;
        border-radius: 8px;
        padding: 8px;
        max-height: 600px;
        overflow-y: auto;
        font-family: 'Monaco', 'Menlo', monospace;
        font-size: 0.75rem;
    }
    
    .log-entry {
        padding: 4px 8px;
        margin: 2px 0;
        border-radius: 4px;
        display: flex;
        gap: 8px;
    }
    
    .log-INFO { background: rgba(0, 168, 132, 0.1); border-left: 3px solid #00a884; }
    .log-WARNING { background: rgba(245, 158, 11, 0.1); border-left: 3px solid #f59e0b; }
    .log-ERROR { background: rgba(239, 68, 68, 0.1); border-left: 3px solid #ef4444; }
    .log-DEBUG { background: rgba(148, 163, 184, 0.1); border-left: 3px solid #64748b; }
    
    .log-time { color: #64748b; min-width: 70px; }
    .log-level { font-weight: 600; min-width: 60px; }
    .log-level-INFO { color: #00a884; }
    .log-level-WARNING { color: #f59e0b; }
    .log-level-ERROR { color: #ef4444; }
    .log-level-DEBUG { color: #64748b; }
    .log-msg { color: #e2e8f0; word-break: break-word; }
    
    /* Agent workflow visualization */
    .workflow-container {
        background: linear-gradient(135deg, #111b21 0%, #0d1418 100%);
        border-radius: 12px;
        padding: 16px;
        margin-bottom: 16px;
    }
    
    .agent-box {
        display: inline-block;
        padding: 12px 20px;
        border-radius: 8px;
        text-align: center;
        min-width: 140px;
        transition: all 0.3s ease;
    }
    
    .agent-pending {
        background: #1a252b;
        border: 2px solid #2d3b45;
        color: #64748b;
    }
    
    .agent-active {
        background: linear-gradient(135deg, #005c4b 0%, #004d40 100%);
        border: 2px solid #00a884;
        color: #e9edef;
        box-shadow: 0 0 20px rgba(0, 168, 132, 0.4);
        animation: pulse 1.5s ease-in-out infinite;
    }
    
    .agent-complete {
        background: #1e3a2f;
        border: 2px solid #00a884;
        color: #00a884;
    }
    
    .agent-error {
        background: #3a1e1e;
        border: 2px solid #ef4444;
        color: #ef4444;
    }
    
    @keyframes pulse {
        0%, 100% { box-shadow: 0 0 20px rgba(0, 168, 132, 0.4); }
        50% { box-shadow: 0 0 30px rgba(0, 168, 132, 0.7); }
    }
    
    .agent-arrow {
        display: inline-block;
        color: #64748b;
        font-size: 1.5rem;
        padding: 0 8px;
        vertical-align: middle;
    }
    
    .agent-arrow-active {
        color: #00a884;
    }
    
    /* MCQ Option buttons */
    .mcq-option {
        background: #2d3b45;
        border: 1px solid #3d4b55;
        color: #e9edef;
        padding: 10px 16px;
        border-radius: 8px;
        margin: 4px;
        cursor: pointer;
        transition: all 0.2s ease;
    }
    
    .mcq-option:hover {
        background: #3d4b55;
        border-color: #f59e0b;
    }
    
    /* KG Visualization */
    .kg-stats-card {
        background: linear-gradient(135deg, #1a252b 0%, #151e24 100%);
        border-radius: 12px;
        padding: 16px;
        text-align: center;
    }
    
    .kg-stat-value {
        font-size: 2rem;
        font-weight: 700;
        color: #00a884;
    }
    
    .kg-stat-label {
        color: #64748b;
        font-size: 0.85rem;
    }
</style>
"""

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# ============================================================================
# SESSION STATE INITIALIZATION
# ============================================================================

def init_session_state():
    """Initialize all session state variables"""
    defaults = {
        # Chat state
        "messages": [],
        "pending_clarification": False,
        "clarification_request": None,
        "original_query": "",
        "clarifications": {},
        "selected_option": None,  # For MCQ callback pattern
        
        # Agent state
        "current_agent": None,  # "agent_1", "agent_2", "agent_3", None
        "agent_status": {
            "agent_1": "pending",  # pending, active, complete, error
            "agent_2": "pending",
            "agent_3": "pending"
        },
        "agent_results": {
            "agent_1": None,
            "agent_2": None,
            "agent_3": None
        },
        
        # KG state
        "kg_loaded": False,
        "kg_info": {},
        "kg_uuid": None,
        "kg_data": None,  # Full KG for visualization
        
        # Service state
        "agent_service": None,
        "source_conn": None,
        "kg_conn": None,
        
        # Logs
        "logs": [],
        "logs_expanded": True,
        
        # UI state
        "current_page": "chat",
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


# ============================================================================
# AGENT WORKFLOW VISUALIZATION
# ============================================================================

def render_agent_workflow():
    """Render the animated 3-agent workflow diagram"""
    status = st.session_state.agent_status
    
    def get_agent_class(agent_key):
        s = status.get(agent_key, "pending")
        if s == "active":
            return "agent-active"
        elif s == "complete":
            return "agent-complete"
        elif s == "error":
            return "agent-error"
        return "agent-pending"
    
    def get_arrow_class(from_agent, to_agent):
        from_status = status.get(from_agent, "pending")
        if from_status in ["complete", "active"]:
            return "agent-arrow-active"
        return ""
    
    workflow_html = f"""
    <div class="workflow-container">
        <div style="text-align: center; margin-bottom: 8px;">
            <span style="color: #64748b; font-size: 0.8rem;">Agent Workflow</span>
        </div>
        <div style="display: flex; justify-content: center; align-items: center; flex-wrap: wrap; gap: 4px;">
            <div class="agent-box {get_agent_class('agent_1')}">
                <div style="font-weight: 600;">Schema Selector</div>
                <div style="font-size: 0.75rem; opacity: 0.7;">Agent 1</div>
            </div>
            <span class="agent-arrow {get_arrow_class('agent_1', 'agent_2')}">→</span>
            <div class="agent-box {get_agent_class('agent_2')}">
                <div style="font-weight: 600;">SQL Generator</div>
                <div style="font-size: 0.75rem; opacity: 0.7;">Agent 2</div>
            </div>
            <span class="agent-arrow {get_arrow_class('agent_2', 'agent_3')}">→</span>
            <div class="agent-box {get_agent_class('agent_3')}">
                <div style="font-weight: 600;">Executor</div>
                <div style="font-size: 0.75rem; opacity: 0.7;">Agent 3</div>
            </div>
        </div>
    </div>
    """
    st.markdown(workflow_html, unsafe_allow_html=True)


def reset_agent_status():
    """Reset all agent statuses to pending"""
    st.session_state.agent_status = {
        "agent_1": "pending",
        "agent_2": "pending",
        "agent_3": "pending"
    }
    st.session_state.agent_results = {
        "agent_1": None,
        "agent_2": None,
        "agent_3": None
    }
    st.session_state.current_agent = None


def update_agent_status(agent: str, status: str, result: Any = None):
    """Update agent status for workflow visualization"""
    st.session_state.agent_status[agent] = status
    st.session_state.current_agent = agent if status == "active" else None
    if result:
        st.session_state.agent_results[agent] = result


# ============================================================================
# LOGS PANEL
# ============================================================================

def render_logs_panel():
    """Render the collapsible logs panel"""
    logs = st.session_state.get("logs", [])
    
    with st.expander("📋 Backend Logs", expanded=st.session_state.logs_expanded):
        if not logs:
            st.caption("No logs yet. Logs will appear here when you interact with the system.")
        else:
            # Control buttons
            col1, col2 = st.columns([1, 1])
            with col1:
                if st.button("🗑️ Clear", key="clear_logs", use_container_width=True):
                    st.session_state.logs = []
                    st.rerun()
            with col2:
                auto_scroll = st.checkbox("Auto-scroll", value=True, key="auto_scroll")
            
            # Render logs
            logs_html = '<div class="logs-container">'
            for log in logs[-100:]:  # Show last 100 logs
                level = log.get("level", "INFO")
                logs_html += f'''
                <div class="log-entry log-{level}">
                    <span class="log-time">{log.get("time", "")}</span>
                    <span class="log-level log-level-{level}">{level}</span>
                    <span class="log-msg">{log.get("msg", "")}</span>
                </div>
                '''
            logs_html += '</div>'
            
            st.markdown(logs_html, unsafe_allow_html=True)


# ============================================================================
# CHAT RENDERING
# ============================================================================

def render_message(msg: Dict):
    """Render a single chat message in WhatsApp style"""
    role = msg.get("role", "")
    content = msg.get("content", "")
    timestamp = msg.get("timestamp", datetime.now().strftime("%H:%M"))
    
    if role == "user":
        st.markdown(f'''
        <div class="user-bubble">
            {content}
            <div class="msg-time">{timestamp}</div>
        </div>
        ''', unsafe_allow_html=True)
    
    elif role == "assistant":
        if msg.get("is_error"):
            st.markdown(f'''
            <div class="error-bubble">
                <div style="color: #ef4444; font-weight: 600; margin-bottom: 8px;">❌ Error</div>
                {content}
                <div class="msg-time">{timestamp}</div>
            </div>
            ''', unsafe_allow_html=True)
        elif msg.get("sql"):
            # Result message with SQL and data
            render_result_message(msg, timestamp)
        else:
            st.markdown(f'''
            <div class="assistant-bubble">
                {content}
                <div class="msg-time">{timestamp}</div>
            </div>
            ''', unsafe_allow_html=True)
    
    elif role == "clarification":
        render_clarification_message(msg, timestamp)


def render_result_message(msg: Dict, timestamp: str):
    """Render a successful result message"""
    content = msg.get("content", "")
    sql = msg.get("sql", "")
    results = msg.get("results", [])
    metadata = msg.get("metadata", {})
    
    st.markdown(f'''
    <div class="results-bubble">
        <div style="color: #00a884; font-weight: 600; margin-bottom: 8px;">✅ Query Successful</div>
        {content}
        <div class="msg-time">{timestamp}</div>
    </div>
    ''', unsafe_allow_html=True)
    
    # SQL expander
    if sql:
        with st.expander("📝 Generated SQL", expanded=True):
            st.code(sql, language="sql")
    
    # Results table
    if results:
        with st.expander(f"📊 Results ({len(results)} rows)", expanded=True):
            df = pd.DataFrame(results)
            st.dataframe(df, use_container_width=True, height=min(400, len(results) * 40 + 50))
    
    # Metadata
    if metadata:
        with st.expander("ℹ️ Metadata"):
            timing = metadata.get("timing", {})
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                tables = metadata.get("tables_used", [])
                st.metric("Tables", len(tables))
                if tables:
                    st.caption(", ".join(tables))
            with col2:
                conf = metadata.get("confidence_score", 0)
                st.metric("Confidence", f"{conf:.2f}" if conf else "N/A")
            with col3:
                st.metric("Iterations", metadata.get("iterations", 1))
            with col4:
                st.metric("Total Time", f"{timing.get('total_ms', 0)}ms")


def render_clarification_message(msg: Dict, timestamp: str):
    """Render a clarification MCQ message"""
    content = msg.get("content", {})
    resolved = msg.get("resolved", False)
    selected = msg.get("selected_answer")
    
    question = content.get("question", "Clarification needed")
    options = content.get("options", [])
    ambiguity = content.get("ambiguity", "")
    
    if resolved and selected:
        # Show resolved clarification
        st.markdown(f'''
        <div class="clarification-bubble" style="opacity: 0.7;">
            <div class="clarification-title">🤔 Clarification (Resolved)</div>
            <div>{question}</div>
            <div style="margin-top: 8px; padding: 8px; background: rgba(0,168,132,0.2); border-radius: 6px; color: #00a884;">
                ✓ Selected: {selected}
            </div>
            <div class="msg-time">{timestamp}</div>
        </div>
        ''', unsafe_allow_html=True)
    else:
        # Show active clarification with buttons
        st.markdown(f'''
        <div class="clarification-bubble">
            <div class="clarification-title">🤔 Clarification Needed</div>
            <div>{question}</div>
            {f'<div style="font-size: 0.8rem; color: #64748b; margin-top: 8px;">Ambiguity: {ambiguity}</div>' if ambiguity else ''}
            <div class="msg-time">{timestamp}</div>
        </div>
        ''', unsafe_allow_html=True)
        
        # Render option buttons
        if options:
            st.write("**Select an option:**")
            cols = st.columns(min(len(options), 4))
            for i, option in enumerate(options):
                with cols[i % len(cols)]:
                    if st.button(
                        option, 
                        key=f"mcq_{hash(question)}_{i}",
                        use_container_width=True,
                        on_click=lambda o=option: setattr(st.session_state, 'selected_option', o)
                    ):
                        pass
        else:
            # Fallback to text input if no options
            user_input = st.text_input(
                "Your response:", 
                key=f"clarify_input_{hash(question)}",
                placeholder="Type your clarification..."
            )
            if st.button("Submit", key=f"clarify_submit_{hash(question)}"):
                if user_input:
                    st.session_state.selected_option = user_input


# ============================================================================
# KNOWLEDGE GRAPH VISUALIZATION
# ============================================================================

def render_kg_graph_view():
    """Render interactive Knowledge Graph visualization"""
    kg_data = st.session_state.kg_data
    
    if not kg_data:
        st.info("No Knowledge Graph loaded. Build one first in the KG Builder page.")
        return
    
    try:
        from streamlit_agraph import agraph, Node, Edge, Config
        
        nodes = []
        edges = []
        
        # Create nodes for tables
        tables = kg_data.get("tables", {})
        for table_name, table_info in tables.items():
            col_count = len(table_info.get("columns", {}))
            nodes.append(Node(
                id=table_name,
                label=f"{table_name}\n({col_count} cols)",
                size=25 + col_count,
                color="#005c4b",
                font={"color": "#ffffff"}
            ))
        
        # Create edges for relationships
        relationships = kg_data.get("relationships", [])
        for rel in relationships:
            from_table = rel.get("from_table_name", rel.get("from_table", ""))
            to_table = rel.get("to_table_name", rel.get("to_table", ""))
            from_col = rel.get("from_column", "")
            to_col = rel.get("to_column", "")
            
            if from_table and to_table:
                edges.append(Edge(
                    source=from_table,
                    target=to_table,
                    label=f"{from_col}→{to_col}",
                    color="#64748b"
                ))
        
        # Graph config
        config = Config(
            width=700,
            height=500,
            directed=True,
            physics=True,
            hierarchical=False,
            nodeHighlightBehavior=True,
            highlightColor="#00a884",
            collapsible=True,
        )
        
        # Render graph
        selected_node = agraph(nodes=nodes, edges=edges, config=config)
        
        # Show selected node details
        if selected_node and selected_node in tables:
            render_table_details(selected_node, tables[selected_node])
            
    except ImportError:
        st.warning("streamlit-agraph not installed. Install it with: `pip install streamlit-agraph`")
        st.info("Showing JSON view instead.")
        render_kg_json_view()


def render_table_details(table_name: str, table_info: Dict):
    """Render details for a selected table"""
    st.subheader(f"📋 {table_name}")
    
    col1, col2 = st.columns(2)
    with col1:
        st.write("**Description:**")
        st.caption(table_info.get("description", "No description"))
    with col2:
        st.write("**Business Domain:**")
        st.caption(table_info.get("business_domain", "Unknown"))
    
    # Columns
    columns = table_info.get("columns", {})
    if columns:
        st.write("**Columns:**")
        col_data = []
        for col_name, col_info in columns.items():
            col_data.append({
                "Column": col_name,
                "Type": col_info.get("data_type", ""),
                "PK": "✓" if col_info.get("is_primary_key") else "",
                "FK": "✓" if col_info.get("is_foreign_key") else "",
                "Nullable": "✓" if col_info.get("is_nullable") else ""
            })
        st.dataframe(pd.DataFrame(col_data), use_container_width=True, hide_index=True)


def render_kg_json_view():
    """Render JSON tree view of KG structure"""
    kg_data = st.session_state.kg_data
    
    if not kg_data:
        st.info("No Knowledge Graph loaded.")
        return
    
    st.json(kg_data, expanded=False)


def render_kg_stats():
    """Render KG statistics cards"""
    kg_info = st.session_state.kg_info
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f'''
        <div class="kg-stats-card">
            <div class="kg-stat-value">{kg_info.get("tables", 0)}</div>
            <div class="kg-stat-label">Tables</div>
        </div>
        ''', unsafe_allow_html=True)
    
    with col2:
        st.markdown(f'''
        <div class="kg-stats-card">
            <div class="kg-stat-value">{kg_info.get("relationships", 0)}</div>
            <div class="kg-stat-label">Relationships</div>
        </div>
        ''', unsafe_allow_html=True)
    
    with col3:
        st.markdown(f'''
        <div class="kg-stats-card">
            <div class="kg-stat-value">{kg_info.get("columns", 0)}</div>
            <div class="kg-stat-label">Columns</div>
        </div>
        ''', unsafe_allow_html=True)
    
    with col4:
        status = "Ready" if st.session_state.kg_loaded else "Not Loaded"
        color = "#00a884" if st.session_state.kg_loaded else "#ef4444"
        st.markdown(f'''
        <div class="kg-stats-card">
            <div class="kg-stat-value" style="color: {color}; font-size: 1.2rem;">●</div>
            <div class="kg-stat-label">{status}</div>
        </div>
        ''', unsafe_allow_html=True)


# ============================================================================
# QUERY PROCESSING WITH AGENT HOOKS
# ============================================================================

def process_query_with_agents(user_query: str, clarifications: dict = None) -> Dict:
    """
    Process a query through the agent service with status updates.
    Returns the response dict from agent_service.query()
    """
    agent_service = st.session_state.agent_service
    if not agent_service:
        return {"success": False, "error": "Agent service not initialized"}
    
    try:
        # Reset agent status
        reset_agent_status()
        
        # Log start
        logger = logging.getLogger(__name__)
        logger.info(f"Processing query: {user_query[:50]}...")
        
        # If we have clarifications, skip ambiguity detection
        if clarifications:
            logger.info(f"Using clarifications: {clarifications}")
        
        # Call agent service
        response = agent_service.query(
            user_query=user_query,
            kg_id=st.session_state.kg_uuid,
            clarifications=clarifications
        )
        
        # Update status based on response
        if response.get("needs_clarification"):
            logger.info("Agent detected ambiguity, requesting clarification")
        elif response.get("success"):
            # Mark all agents as complete
            st.session_state.agent_status = {
                "agent_1": "complete",
                "agent_2": "complete", 
                "agent_3": "complete"
            }
            logger.info("Query executed successfully")
        else:
            logger.error(f"Query failed: {response.get('error', 'Unknown')}")
        
        return response
        
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Query processing error: {str(e)}")
        return {"success": False, "error": str(e)}


def handle_clarification_selection(selected_option: str):
    """Handle when user selects a clarification option"""
    logger = logging.getLogger(__name__)
    logger.info(f"User selected clarification: {selected_option}")
    
    # Find and mark the clarification as resolved
    for msg in st.session_state.messages:
        if msg.get("role") == "clarification" and not msg.get("resolved"):
            msg["resolved"] = True
            msg["selected_answer"] = selected_option
            break
    
    # Store the clarification
    if st.session_state.clarification_request:
        question = st.session_state.clarification_request.get("question", "clarification")
    st.session_state.clarifications[question] = selected_option
    
    # Add user's selection to messages
    st.session_state.messages.append({
        "role": "user",
        "content": selected_option,
        "timestamp": datetime.now().strftime("%H:%M")
    })
    
    # Re-process query with accumulated clarifications
    response = process_query_with_agents(
            st.session_state.original_query,
            st.session_state.clarifications
        )
    
    # Handle response
    if response.get("needs_clarification"):
        st.session_state.clarification_request = response["clarification_request"]
        st.session_state.pending_clarification = True
        st.session_state.messages.append({
            "role": "clarification",
            "content": response["clarification_request"],
            "timestamp": datetime.now().strftime("%H:%M"),
            "resolved": False
        })
    else:
        # Clarification complete
        st.session_state.pending_clarification = False
        st.session_state.clarification_request = None
        
        if response.get("success"):
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"Found {len(response.get('data', []))} results",
                "sql": response.get("sql", ""),
                "results": response.get("data", []),
                "metadata": response.get("metadata", {}),
                "timestamp": datetime.now().strftime("%H:%M")
            })
        else:
            st.session_state.messages.append({
                "role": "assistant",
                "content": response.get("error", "Unknown error"),
                "is_error": True,
                "timestamp": datetime.now().strftime("%H:%M")
            })
        
        # Clear clarification state
        st.session_state.original_query = ""
        st.session_state.clarifications = {}


# ============================================================================
# PAGES
# ============================================================================

def chat_page():
    """Main chat interface"""
    st.header("💬 SQL Assistant")
    
    # Check for pending MCQ selection (must be processed before rendering)
    if st.session_state.selected_option:
        handle_clarification_selection(st.session_state.selected_option)
        st.session_state.selected_option = None
        st.rerun()
    
    if not st.session_state.kg_loaded:
        st.warning("⚠️ No Knowledge Graph loaded. Go to **KG Builder** to create one first.")
        return
    
    # Agent workflow visualization
    render_agent_workflow()
    
    st.caption(f"Connected to: **{st.session_state.kg_info.get('db_name', 'Unknown')}**")
    
    # Chat container
    chat_container = st.container()
    
    with chat_container:
        # Render all messages
        for msg in st.session_state.messages:
            render_message(msg)
    
    # Don't show input if waiting for MCQ selection
    if st.session_state.pending_clarification:
        st.info("👆 Please select an option above to continue")
        return
    
    st.divider()
    
    # Chat input
    with st.form("chat_form", clear_on_submit=True):
        user_input = st.text_input(
            "Ask a question",
            placeholder="e.g., Show me the top 5 customers by total purchase amount",
            label_visibility="collapsed"
        )
        col1, col2 = st.columns([5, 1])
        with col2:
            submit = st.form_submit_button("Send", use_container_width=True)
    
    if submit and user_input:
        # Add user message
        st.session_state.messages.append({
            "role": "user",
            "content": user_input,
            "timestamp": datetime.now().strftime("%H:%M")
        })
        
        # Process query
        with st.spinner("🔄 Processing..."):
            response = process_query_with_agents(user_input)
        
        # Handle response
        if response.get("needs_clarification"):
            st.session_state.pending_clarification = True
            st.session_state.clarification_request = response["clarification_request"]
            st.session_state.original_query = user_input
            st.session_state.clarifications = {}
            
            st.session_state.messages.append({
                "role": "clarification",
                "content": response["clarification_request"],
                "timestamp": datetime.now().strftime("%H:%M"),
                "resolved": False
            })
        
        elif response.get("success"):
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"Found {len(response.get('data', []))} results",
                "sql": response.get("sql", ""),
                "results": response.get("data", []),
                "metadata": response.get("metadata", {}),
                "timestamp": datetime.now().strftime("%H:%M")
            })
        else:
            st.session_state.messages.append({
                "role": "assistant",
                "content": response.get("error", "Unknown error"),
                "is_error": True,
                "timestamp": datetime.now().strftime("%H:%M")
            })
        
        st.rerun()


def kg_builder_page():
    """KG Builder interface"""
    st.header("🔨 Knowledge Graph Builder")
    
    # Show current KG status
    if st.session_state.kg_loaded:
        render_kg_stats()
        st.success(f"✅ Knowledge Graph loaded: `{st.session_state.kg_info.get('db_name')}`")
        st.divider()
    
    st.subheader("Source Database Connection")
    st.caption("Enter the credentials for the database you want to query.")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        db_host = st.text_input("Host", value="localhost", key="db_host")
    with col2:
        db_port = st.text_input("Port", value="5432", key="db_port")
    
    db_name = st.text_input("Database Name", placeholder="my_database", key="db_name")
    db_schema = st.text_input("Schema", value="public", key="db_schema")
    
    col1, col2 = st.columns(2)
    with col1:
        db_user = st.text_input("Username", value="postgres", key="db_user")
    with col2:
        db_password = st.text_input("Password", type="password", key="db_password")
    
    st.divider()
    st.info("💡 KG storage database and OpenAI API key are configured via `.env` file.")
    
    if st.button("🔨 Build Knowledge Graph", use_container_width=True, type="primary"):
        if not db_name:
            st.error("Please enter a database name")
            return
        build_kg(db_host, db_port, db_name, db_schema, db_user, db_password)


def build_kg(host, port, db_name, schema, user, password):
    """Build Knowledge Graph - matches scripts/build_kg.py flow"""
    progress_bar = st.progress(0)
    status_text = st.empty()
    logger = logging.getLogger(__name__)
    
    try:
        from config.settings import Settings
        from src.openai_client import OpenAIClient
        from src.kg.builders.kg_builder import KGBuilder
        from src.kg.manager.kg_manager import KGManager
        from src.api.agent_service import AgentService
        import psycopg2
        
        settings = Settings()
        
        logger.info("Starting KG build process...")
        status_text.text("Connecting to source database...")
        progress_bar.progress(10)
        
        source_conn = psycopg2.connect(
            host=host,
            port=int(port),
            database=db_name,
            user=user,
            password=password
        )
        logger.info(f"Connected to source database: {db_name}")
        
        status_text.text("Connecting to KG storage...")
        progress_bar.progress(20)
        
        kg_conn = psycopg2.connect(
            host=settings.KG_HOST,
            port=settings.KG_PORT,
            database=settings.KG_DATABASE,
            user=settings.KG_USER,
            password=settings.KG_PASSWORD
        )
        logger.info(f"Connected to KG storage: {settings.KG_DATABASE}")
        
        status_text.text("Initializing OpenAI client...")
        progress_bar.progress(30)
        
        openai_client = OpenAIClient(
            api_key=settings.OPENAI_API_KEY,
            enable_langfuse=settings.enable_langfuse
        )
        logger.info("OpenAI client initialized")
        
        status_text.text("Building Knowledge Graph (this may take a few minutes)...")
        progress_bar.progress(40)
        
        builder = KGBuilder(
            source_conn=source_conn,
            kg_conn=kg_conn,
            openai_client=openai_client,
            settings=settings
        )
        
        logger.info("KGBuilder initialized, starting build...")
        
        kg = builder.build_kg(
            source_db_name=db_name,
            source_db_host=host,
            source_db_port=int(port),
            schema_name=schema,
            generate_descriptions=True,
            generate_embeddings=True
        )
        
        progress_bar.progress(85)
        
        if kg:
            status_text.text("Setting up agent service...")
            logger.info(f"KG built successfully: {kg.kg_id}")
            
            kg_manager = KGManager(kg_conn, settings.CHROMA_PERSIST_DIR)
            
            st.session_state.agent_service = AgentService(
                kg_manager=kg_manager,
                openai_client=openai_client,
                source_db_conn=source_conn,
                kg_conn=kg_conn
            )
            st.session_state.kg_uuid = kg.kg_id
            st.session_state.kg_loaded = True
            
            # Count columns
            total_columns = sum(len(t.columns) for t in kg.tables.values())
            
            st.session_state.kg_info = {
                "db_name": kg.source_db_name,
                "tables": len(kg.tables),
                "relationships": len(kg.relationships),
                "columns": total_columns
            }
            st.session_state.source_conn = source_conn
            st.session_state.kg_conn = kg_conn
            
            # Store KG data for visualization
            st.session_state.kg_data = extract_kg_data_for_viz(kg)
            
            progress_bar.progress(100)
            status_text.text("Complete!")
            logger.info("Agent service initialized successfully")
            
            st.success(f"✅ Knowledge Graph ready! ({len(kg.tables)} tables, {len(kg.relationships)} relationships)")
            st.balloons()
        else:
            logger.error("KG build returned None")
            st.error("Failed to build Knowledge Graph")
            
    except Exception as e:
        logger.error(f"KG build failed: {str(e)}")
        st.error(f"Error: {str(e)}")
        progress_bar.empty()
        status_text.empty()


def extract_kg_data_for_viz(kg) -> Dict:
    """Extract KG data in a format suitable for visualization"""
    data = {
        "tables": {},
        "relationships": []
    }
    
    for table_name, table in kg.tables.items():
        data["tables"][table_name] = {
            "description": table.description,
            "business_domain": table.business_domain,
            "row_count": table.row_count_estimate,
            "columns": {}
        }
        for col_name, col in table.columns.items():
            data["tables"][table_name]["columns"][col_name] = {
                "data_type": col.data_type,
                "is_primary_key": col.is_primary_key,
                "is_foreign_key": col.is_foreign_key,
                "is_nullable": col.is_nullable,
                "description": col.description
            }
    
    for rel in kg.relationships:
        data["relationships"].append({
            "from_table_name": rel.from_table_name,
            "to_table_name": rel.to_table_name,
            "from_column": rel.from_column,
            "to_column": rel.to_column,
            "relationship_type": rel.relationship_type
        })
    
    return data


def kg_viewer_page():
    """Knowledge Graph Viewer page"""
    st.header("🔍 Knowledge Graph Viewer")
    
    if not st.session_state.kg_loaded:
        st.info("No Knowledge Graph loaded. Build one first in the KG Builder page.")
        return
    
    # Stats
    render_kg_stats()
    
    st.divider()
    
    # Tabs for different views
    tab1, tab2, tab3 = st.tabs(["📊 Graph View", "📄 JSON View", "📋 Table Details"])
    
    with tab1:
        render_kg_graph_view()
    
    with tab2:
        render_kg_json_view()
    
    with tab3:
        kg_data = st.session_state.kg_data
        if kg_data:
            tables = list(kg_data.get("tables", {}).keys())
            if tables:
                selected = st.selectbox("Select a table", tables)
                if selected:
                    render_table_details(selected, kg_data["tables"][selected])


def history_page():
    """Query history page"""
    st.header("📜 Query History")
    
    if not st.session_state.messages:
        st.info("No queries yet. Go to Chat to ask questions.")
        return
    
    # Filter to show only Q&A pairs
    for i, msg in enumerate(st.session_state.messages):
        if msg["role"] == "user":
            st.markdown(f"**Q{i+1}:** {msg['content']}")
        elif msg["role"] == "assistant" and msg.get("sql"):
            with st.expander("View SQL & Results"):
                st.code(msg["sql"], language="sql")
                if msg.get("results"):
                    st.dataframe(pd.DataFrame(msg["results"]), use_container_width=True)
            st.divider()


def settings_page():
    """Settings page"""
    st.header("⚙️ Settings")
    
    st.subheader("Display Settings")
    st.checkbox("Show SQL in results", value=True, key="show_sql")
    st.checkbox("Show metadata in results", value=True, key="show_metadata")
    st.number_input("Max rows to display", min_value=10, max_value=1000, value=100, key="max_rows")
    
    st.divider()
    
    st.subheader("Logs Settings")
    st.session_state.logs_expanded = st.checkbox(
        "Logs panel expanded by default", 
        value=st.session_state.logs_expanded
    )
    
    st.divider()
    
    st.subheader("Clear Data")
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("🗑️ Clear Chat", use_container_width=True):
            st.session_state.messages = []
            st.session_state.pending_clarification = False
            st.session_state.clarification_request = None
            reset_agent_status()
            st.success("Chat cleared!")
            st.rerun()
    
    with col2:
        if st.button("📋 Clear Logs", use_container_width=True):
            st.session_state.logs = []
            st.success("Logs cleared!")
            st.rerun()
    
    with col3:
        if st.button("🔌 Disconnect", use_container_width=True):
            st.session_state.kg_loaded = False
            st.session_state.agent_service = None
            st.session_state.kg_uuid = None
            st.session_state.kg_info = {}
            st.session_state.kg_data = None
            st.session_state.messages = []
            reset_agent_status()
            st.success("Disconnected!")
            st.rerun()


# ============================================================================
# SIDEBAR
# ============================================================================

def render_sidebar():
    """Render the navigation sidebar"""
    st.sidebar.title("🗃️ Text2SQL")
    st.sidebar.caption("Premium Agent UI")
    st.sidebar.divider()
    
    page = st.sidebar.radio(
        "Navigation",
        ["💬 Chat", "🔨 KG Builder", "🔍 KG Viewer", "📜 History", "⚙️ Settings"],
        label_visibility="collapsed"
    )
    
    st.sidebar.divider()
    
    # Connection status
    if st.session_state.kg_loaded:
        st.sidebar.success(f"""
        **● Connected**  
        **{st.session_state.kg_info.get('db_name', 'Unknown')}**  
        {st.session_state.kg_info.get('tables', 0)} tables  
        {st.session_state.kg_info.get('relationships', 0)} relations
        """)
    else:
        st.sidebar.error("● Not Connected\n\nBuild KG to connect")
    
    return page


# ============================================================================
# MAIN LAYOUT
# ============================================================================

def main():
    """Main application entry point"""
    # Initialize
    init_session_state()
    setup_logging()
    
    # Sidebar navigation
    page = render_sidebar()
    
    # Main content area with logs panel
    main_col, logs_col = st.columns([4, 1])
    
    with main_col:
        if page == "💬 Chat":
            chat_page()
        elif page == "🔨 KG Builder":
            kg_builder_page()
        elif page == "🔍 KG Viewer":
            kg_viewer_page()
        elif page == "📜 History":
            history_page()
        elif page == "⚙️ Settings":
            settings_page()
    
    with logs_col:
        render_logs_panel()


if __name__ == "__main__":
    main()
