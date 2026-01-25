"""
Text2SQL Agent - Production Streamlit UI
A powerful natural language to SQL conversion system with Knowledge Graph visualization.
"""

import os
import sys
import json
import time
import logging
from pathlib import Path
from uuid import UUID
from datetime import datetime
from typing import Dict, List, Any, Optional
import threading
from queue import Queue

import streamlit as st
import pandas as pd

# Add parent directory to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import main module functions
from main import (
    setup_logging,
    get_kg_connection,
    get_connections,
    connect_or_build_kg,
    list_knowledge_graphs,
    get_agent_service,
    process_query,
    submit_feedback,
    get_log_file_path,
    clear_agent_service_cache,
    ProgressUpdate,
    ConnectionResult,
    KGLoadResult,
    QueryResult,
    KGListItem
)

# Page configuration
st.set_page_config(
    page_title="Text2SQL Agent",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)


def load_custom_css():
    """Load production-quality minimal CSS"""
    st.markdown("""
    <style>
    /* Root variables for consistent theming */
    :root {
        --primary: #2563eb;
        --primary-hover: #1d4ed8;
        --success: #059669;
        --warning: #d97706;
        --error: #dc2626;
        --gray-50: #f9fafb;
        --gray-100: #f3f4f6;
        --gray-200: #e5e7eb;
        --gray-300: #d1d5db;
        --gray-400: #9ca3af;
        --gray-500: #6b7280;
        --gray-600: #4b5563;
        --gray-700: #374151;
        --gray-800: #1f2937;
        --gray-900: #111827;
    }
    
    /* Clean main container */
    .main .block-container {
        padding: 1.5rem 2rem;
        max-width: 1400px;
    }
    
    /* Sidebar - clean dark theme */
    [data-testid="stSidebar"] {
        background: var(--gray-900);
    }
    
    [data-testid="stSidebar"] * {
        color: var(--gray-300) !important;
    }
    
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {
        color: white !important;
    }
    
    /* Header - subtle gradient */
    .app-header {
        background: var(--gray-800);
        padding: 1.25rem 1.5rem;
        border-radius: 8px;
        margin-bottom: 1.5rem;
        border-left: 4px solid var(--primary);
    }
    
    .app-header h1 {
        color: white;
        margin: 0;
        font-size: 1.5rem;
        font-weight: 600;
    }
    
    .app-header p {
        color: var(--gray-400);
        margin: 0.25rem 0 0 0;
        font-size: 0.875rem;
    }
    
    /* Cards */
    .card {
        background: white;
        border-radius: 8px;
        padding: 1.25rem;
        border: 1px solid var(--gray-200);
        margin-bottom: 1rem;
    }
    
    .card-dark {
        background: var(--gray-800);
        border-radius: 8px;
        padding: 1.25rem;
        color: white;
        margin-bottom: 1rem;
    }
    
    /* Chat messages */
    .chat-user {
        background: var(--gray-100);
        border: 1px solid var(--gray-200);
        padding: 1rem 1.25rem;
        border-radius: 8px;
        margin: 0.75rem 0;
        margin-left: 15%;
    }
    
    .chat-user .label {
        font-size: 0.75rem;
        color: var(--gray-500);
        margin-bottom: 0.25rem;
        font-weight: 500;
    }
    
    .chat-assistant {
        background: white;
        border: 1px solid var(--gray-200);
        padding: 1rem 1.25rem;
        border-radius: 8px;
        margin: 0.75rem 0;
        margin-right: 15%;
    }
    
    .chat-assistant .label {
        font-size: 0.75rem;
        color: var(--primary);
        margin-bottom: 0.25rem;
        font-weight: 500;
    }
    
    /* Clarification card */
    .clarification-card {
        background: #fffbeb;
        border: 1px solid #fcd34d;
        border-radius: 8px;
        padding: 1.25rem;
        margin: 1rem 0;
    }
    
    .clarification-card h4 {
        color: var(--gray-800);
        margin: 0 0 0.75rem 0;
        font-size: 0.95rem;
    }
    
    /* SQL code block */
    .sql-block {
        background: var(--gray-900);
        color: #e5e7eb;
        padding: 1rem;
        border-radius: 6px;
        font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace;
        font-size: 0.8rem;
        overflow-x: auto;
        margin: 0.5rem 0;
    }
    
    /* Metrics */
    .metric-row {
        display: flex;
        gap: 1rem;
        margin-bottom: 1rem;
    }
    
    .metric-item {
        background: var(--gray-50);
        border: 1px solid var(--gray-200);
        border-radius: 6px;
        padding: 1rem;
        text-align: center;
        flex: 1;
    }
    
    .metric-value {
        font-size: 1.75rem;
        font-weight: 700;
        color: var(--gray-800);
    }
    
    .metric-label {
        font-size: 0.75rem;
        color: var(--gray-500);
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    /* Status badges */
    .badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 500;
    }
    
    .badge-success {
        background: #d1fae5;
        color: #065f46;
    }
    
    .badge-error {
        background: #fee2e2;
        color: #991b1b;
    }
    
    .badge-warning {
        background: #fef3c7;
        color: #92400e;
    }
    
    /* Feedback section */
    .feedback-section {
        background: var(--gray-50);
        border: 1px solid var(--gray-200);
        border-radius: 6px;
        padding: 0.75rem 1rem;
        margin-top: 0.75rem;
    }
    
    .feedback-buttons {
        display: flex;
        gap: 0.5rem;
        align-items: center;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Button overrides */
    .stButton > button {
        border-radius: 6px;
        font-weight: 500;
        transition: all 0.15s ease;
    }
    
    /* Form inputs */
    .stTextInput > div > div > input,
    .stNumberInput > div > div > input,
    .stSelectbox > div > div {
        border-radius: 6px;
    }
    
    /* Data tables */
    .stDataFrame {
        border-radius: 6px;
        overflow: hidden;
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        font-weight: 500;
        color: var(--gray-700);
    }
    
    /* Sidebar section divider */
    .sidebar-divider {
        border-top: 1px solid var(--gray-700);
        margin: 1rem 0;
    }
    
    /* Graph container */
    .graph-container {
        background: white;
        border: 1px solid var(--gray-200);
        border-radius: 8px;
        overflow: hidden;
    }
    </style>
    """, unsafe_allow_html=True)


def init_session_state():
    """Initialize session state variables"""
    defaults = {
        # Connection state
        "connected": False,
        "kg_conn": None,
        "source_conn": None,
        "settings": None,
        
        # KG state
        "kg_loaded": False,
        "kg_id": None,
        "kg_data": None,
        "kg_info": None,
        
        # Agent state
        "agent_service": None,
        
        # Chat state
        "messages": [],
        "processing": False,
        "pending_clarification": None,  # Stores clarification request
        "selected_clarification": None,  # User's selected option
        
        # UI state
        "active_section": "database",
        "show_sql": True,
        "show_explanation": True,
        
        # Progress
        "current_progress": None,
        
        # Credentials
        "db_credentials": {
            "host": "localhost",
            "port": 5432,
            "database": "",
            "user": "",
            "password": ""
        }
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def progress_callback(update: ProgressUpdate):
    """Callback to handle progress updates"""
    st.session_state.current_progress = {
        "stage": update.stage,
        "message": update.message,
        "progress": update.progress,
        "details": update.details
    }


def render_header():
    """Render the application header"""
    st.markdown("""
    <div class="app-header">
        <h1>⚡ Text2SQL Agent</h1>
        <p>Transform natural language into SQL queries</p>
    </div>
    """, unsafe_allow_html=True)


def render_sidebar():
    """Render the sidebar with navigation and status"""
    with st.sidebar:
        st.markdown("""
        <div style="text-align: center; padding: 1rem 0 1.5rem 0;">
            <span style="font-size: 2rem;">⚡</span>
            <h2 style="margin: 0.5rem 0 0 0; font-size: 1.25rem;">Text2SQL</h2>
            <p style="color: #6b7280; font-size: 0.75rem; margin: 0;">Production v1.0</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Connection Status
        st.markdown("**Status**")
        col1, col2 = st.columns(2)
        with col1:
            if st.session_state.connected:
                st.success("Connected")
            else:
                st.error("Offline")
        with col2:
            if st.session_state.kg_loaded:
                st.success("KG Ready")
            else:
                st.warning("No KG")
        
        st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)
        
        # Navigation
        st.markdown("**Navigation**")
        
        sections = [
            ("database", "Database"),
            ("chat", "Chat"),
            ("knowledge_graph", "Knowledge Graph"),
            ("history", "History")
        ]
        
        for key, label in sections:
            is_active = st.session_state.active_section == key
            if st.button(
                label,
                key=f"nav_{key}",
                use_container_width=True,
                type="primary" if is_active else "secondary"
            ):
                st.session_state.active_section = key
                st.rerun()
        
        st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)
        
        # KG Info
        if st.session_state.kg_loaded and st.session_state.kg_info:
            st.markdown("**Knowledge Graph**")
            info = st.session_state.kg_info
            st.caption(f"Database: {info.get('db_name', 'N/A')}")
            st.caption(f"Tables: {info.get('tables_count', 0)}")
            st.caption(f"Relations: {info.get('relationships_count', 0)}")
            
            st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)
        
        # Settings
        st.markdown("**Display**")
        st.session_state.show_sql = st.checkbox("Show SQL", value=st.session_state.show_sql)
        st.session_state.show_explanation = st.checkbox("Show Explanation", value=st.session_state.show_explanation)
        
        if st.button("Clear Cache", use_container_width=True):
            clear_agent_service_cache()
            st.session_state.agent_service = None
            st.toast("Cache cleared")


def render_database_section():
    """Render the database connection section"""
    st.subheader("Database Connection")
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        st.markdown("Enter your PostgreSQL credentials to connect and build a Knowledge Graph.")
        
        with st.form("db_connection_form"):
            c1, c2 = st.columns(2)
            
            with c1:
                host = st.text_input("Host", value=st.session_state.db_credentials.get("host", "localhost"))
                database = st.text_input("Database", value=st.session_state.db_credentials.get("database", ""))
                user = st.text_input("Username", value=st.session_state.db_credentials.get("user", ""))
            
            with c2:
                port = st.number_input("Port", value=st.session_state.db_credentials.get("port", 5432), min_value=1, max_value=65535)
                password = st.text_input("Password", type="password", value=st.session_state.db_credentials.get("password", ""))
                
                st.markdown("**Options**")
                generate_descriptions = st.checkbox("AI Descriptions", value=True)
                generate_embeddings = st.checkbox("Embeddings", value=True)
            
            submitted = st.form_submit_button("Connect", use_container_width=True, type="primary")
            
            if submitted:
                if not all([host, database, user, password]):
                    st.error("Please fill in all fields")
                else:
                    st.session_state.db_credentials = {
                        "host": host, "port": port, "database": database,
                        "user": user, "password": password
                    }
                    
                    with st.spinner("Connecting..."):
                        result = connect_or_build_kg(
                            source_host=host, source_port=port, source_db=database,
                            source_user=user, source_password=password,
                            generate_descriptions=generate_descriptions,
                            generate_embeddings=generate_embeddings,
                            progress_callback=progress_callback
                        )
                        
                        if result.success:
                            st.session_state.connected = True
                            st.session_state.kg_loaded = True
                            st.session_state.kg_id = result.kg_id
                            st.session_state.kg_data = result.kg_data
                            st.session_state.kg_info = {
                                "db_name": result.db_name or database,
                                "tables_count": result.tables_count,
                                "relationships_count": result.relationships_count,
                                "columns_count": result.columns_count
                            }
                            
                            conn_result = get_connections(
                                source_host=host, source_port=port, source_db=database,
                                source_user=user, source_password=password
                            )
                            
                            if conn_result.success:
                                st.session_state.kg_conn = conn_result.kg_conn
                                st.session_state.source_conn = conn_result.source_conn
                                st.session_state.settings = conn_result.settings
                                st.session_state.agent_service = get_agent_service(
                                    kg_conn=conn_result.kg_conn,
                                    source_conn=conn_result.source_conn,
                                    settings=conn_result.settings
                                )
                            
                            st.success(f"Connected! {result.tables_count} tables loaded.")
                            st.session_state.active_section = "chat"
                            st.rerun()
                        else:
                            st.error(f"Failed: {result.error}")
    
    with col2:
        st.markdown("**Existing Knowledge Graphs**")
        
        kg_conn_result = get_kg_connection()
        if kg_conn_result.success:
            kgs = list_knowledge_graphs(kg_conn_result.kg_conn)
            
            if kgs:
                for kg in kgs[:5]:
                    with st.container():
                        st.markdown(f"""
                        <div class="card">
                            <strong>{kg.db_name}</strong><br>
                            <small style="color: #6b7280;">
                                {kg.db_host}:{kg.db_port} • {kg.tables_count} tables
                            </small>
                        </div>
                        """, unsafe_allow_html=True)
            else:
                st.info("No existing Knowledge Graphs")
            
            kg_conn_result.kg_conn.close()


def render_chat_section():
    """Render the chat interface section"""
    if not st.session_state.kg_loaded:
        st.warning("Please connect to a database first.")
        if st.button("Go to Database", type="primary"):
            st.session_state.active_section = "database"
            st.rerun()
        return
    
    st.subheader("Chat")
    
    # Chat messages container
    chat_container = st.container()
    
    with chat_container:
        for i, msg in enumerate(st.session_state.messages):
            render_chat_message(msg, i)
    
    # Pending clarification handling
    if st.session_state.pending_clarification:
        render_clarification_ui()
        return  # Don't show input while clarification is pending
    
    # Chat input
    st.divider()
    
    col1, col2 = st.columns([6, 1])
    
    with col1:
        user_query = st.chat_input("Ask about your data...")
    
    with col2:
        if st.button("Clear", use_container_width=True):
            st.session_state.messages = []
            st.session_state.pending_clarification = None
            st.rerun()
    
    # Process new query
    if user_query and not st.session_state.processing:
        process_user_query(user_query)


def render_chat_message(msg: Dict, index: int):
    """Render a single chat message with proper formatting"""
    
    if msg["role"] == "user":
        st.markdown(f"""
        <div class="chat-user">
            <div class="label">You</div>
            {msg["content"]}
        </div>
        """, unsafe_allow_html=True)
    
    elif msg["role"] == "assistant":
        with st.container():
            # Main message
            if msg.get("success"):
                icon = "✓"
                status_class = "badge-success"
                status_text = "Success"
            elif msg.get("needs_clarification"):
                icon = "?"
                status_class = "badge-warning"
                status_text = "Clarification Needed"
            else:
                icon = "✗"
                status_class = "badge-error"
                status_text = "Error"
            
            st.markdown(f"""
            <div class="chat-assistant">
                <div class="label">Assistant <span class="badge {status_class}">{status_text}</span></div>
                <div style="margin-top: 0.5rem;">{msg.get("content", "")}</div>
            </div>
            """, unsafe_allow_html=True)
            
            # SQL (if available and enabled)
            if msg.get("sql") and st.session_state.show_sql:
                with st.expander("SQL Query", expanded=False):
                    st.code(msg["sql"], language="sql")
            
            # Data results
            if msg.get("data") and len(msg["data"]) > 0:
                with st.expander(f"Results ({len(msg['data'])} rows)", expanded=True):
                    df = pd.DataFrame(msg["data"])
                    st.dataframe(df, use_container_width=True, hide_index=True)
            
            # Explanation
            if msg.get("explanation") and st.session_state.show_explanation:
                with st.expander("Explanation", expanded=False):
                    st.markdown(msg["explanation"])
            
            # Error details
            if msg.get("error") and not msg.get("needs_clarification"):
                with st.expander("Error Details", expanded=False):
                    st.error(msg["error"])
            
            # Feedback section (only for completed queries)
            if msg.get("success") or (msg.get("error") and not msg.get("needs_clarification")):
                render_feedback_ui(index, msg)


def render_clarification_ui():
    """Render the clarification MCQ interface"""
    clarification = st.session_state.pending_clarification
    
    st.markdown("""
    <div class="clarification-card">
        <h4>🤔 Clarification Needed</h4>
    </div>
    """, unsafe_allow_html=True)
    
    # Show the question
    st.markdown(f"**{clarification['question']}**")
    
    # Show reasoning if available
    if clarification.get("reasoning"):
        with st.expander("Why this question?", expanded=False):
            st.caption(clarification["reasoning"])
    
    # MCQ Options
    options = clarification.get("options", [])
    
    if options:
        selected = st.radio(
            "Select an option:",
            options=options,
            key="clarification_selection",
            label_visibility="collapsed"
        )
        
        col1, col2, col3 = st.columns([1, 1, 4])
        
        with col1:
            if st.button("Submit", type="primary", use_container_width=True):
                # Process with clarification
                process_with_clarification(selected)
        
        with col2:
            if st.button("Skip", use_container_width=True):
                # Process without clarification (force execute)
                st.session_state.pending_clarification = None
                original_query = st.session_state.messages[-1]["content"] if st.session_state.messages else ""
                if original_query:
                    process_user_query(original_query, force=True)
                st.rerun()


def render_feedback_ui(msg_index: int, msg: Dict):
    """Render feedback UI for a message"""
    
    feedback_key = f"feedback_{msg_index}"
    rating_key = f"rating_{msg_index}"
    submitted_key = f"feedback_submitted_{msg_index}"
    
    # Check if already submitted
    if st.session_state.get(submitted_key):
        st.caption("✓ Feedback submitted")
        return
    
    with st.container():
        st.markdown('<div class="feedback-section">', unsafe_allow_html=True)
        
        col1, col2, col3, col4 = st.columns([1, 1, 1, 3])
        
        with col1:
            thumbs_up = st.button("👍", key=f"thumbs_up_{msg_index}", help="Helpful")
        
        with col2:
            thumbs_down = st.button("👎", key=f"thumbs_down_{msg_index}", help="Not helpful")
        
        with col3:
            show_feedback = st.button("💬", key=f"show_feedback_{msg_index}", help="Add comment")
        
        # Handle quick feedback
        if thumbs_up:
            submit_query_feedback(msg_index, "Helpful", 5)
        
        if thumbs_down:
            submit_query_feedback(msg_index, "Not helpful", 1)
        
        # Show detailed feedback form
        if show_feedback or st.session_state.get(f"show_feedback_form_{msg_index}"):
            st.session_state[f"show_feedback_form_{msg_index}"] = True
            
            feedback_text = st.text_area(
                "Your feedback",
                key=feedback_key,
                placeholder="What could be improved?",
                height=80
            )
            
            rating = st.slider("Rating", 1, 5, 3, key=rating_key)
            
            if st.button("Submit Feedback", key=f"submit_fb_{msg_index}"):
                submit_query_feedback(msg_index, feedback_text, rating)
        
        st.markdown('</div>', unsafe_allow_html=True)


def submit_query_feedback(msg_index: int, feedback_text: str, rating: int):
    """Submit feedback for a query"""
    try:
        msg = st.session_state.messages[msg_index]
        query_log_id = msg.get("metadata", {}).get("query_log_id")
        
        if query_log_id and st.session_state.agent_service:
            result = submit_feedback(
                agent_service=st.session_state.agent_service,
                query_log_id=query_log_id,
                feedback=feedback_text,
                rating=rating
            )
            
            if result.success:
                st.session_state[f"feedback_submitted_{msg_index}"] = True
                st.toast("Feedback submitted!")
            else:
                st.toast("Feedback saved locally")
                st.session_state[f"feedback_submitted_{msg_index}"] = True
        else:
            # No query log ID, just mark as submitted
            st.session_state[f"feedback_submitted_{msg_index}"] = True
            st.toast("Thank you for your feedback!")
        
        st.rerun()
        
    except Exception as e:
        st.error(f"Failed to submit feedback: {e}")


def process_user_query(user_query: str, force: bool = False, clarifications: Dict = None):
    """Process a user query through the agent"""
    st.session_state.processing = True
    
    # Add user message (only if not a clarification response)
    if not clarifications:
        st.session_state.messages.append({
            "role": "user",
            "content": user_query
        })
    
    with st.spinner("Processing..."):
        try:
            result = process_query(
                agent_service=st.session_state.agent_service,
                kg_id=st.session_state.kg_id,
                user_query=user_query,
                clarifications=clarifications,
                progress_callback=progress_callback
            )
            
            # Build response message
            response_msg = {
                "role": "assistant",
                "content": "",
                "sql": result.sql,
                "data": result.data,
                "explanation": result.explanation,
                "error": result.error,
                "success": result.success,
                "needs_clarification": result.needs_clarification,
                "metadata": result.metadata
            }
            
            if result.success:
                row_count = len(result.data) if result.data else 0
                response_msg["content"] = f"Query executed successfully. Found {row_count} results."
                st.session_state.pending_clarification = None
                
            elif result.needs_clarification and not force:
                # Store clarification request
                clarification_data = result.clarification_request
                st.session_state.pending_clarification = {
                    "question": clarification_data.get("question", "Please clarify your query"),
                    "options": clarification_data.get("options", []),
                    "ambiguity": clarification_data.get("ambiguity", ""),
                    "reasoning": clarification_data.get("reasoning", ""),
                    "original_query": user_query
                }
                response_msg["content"] = f"I need clarification to better understand your query."
                
            else:
                response_msg["content"] = f"Query failed: {result.error}"
                st.session_state.pending_clarification = None
            
            st.session_state.messages.append(response_msg)
            
        except Exception as e:
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"An error occurred: {str(e)}",
                "error": str(e),
                "success": False
            })
            st.session_state.pending_clarification = None
    
    st.session_state.processing = False
    st.rerun()


def process_with_clarification(selected_option: str):
    """Process the original query with the user's clarification"""
    
    clarification = st.session_state.pending_clarification
    original_query = clarification.get("original_query", "")
    question = clarification.get("question", "clarification")
    
    # Build clarifications dict
    clarifications = {question: selected_option}
    
    # Clear pending clarification
    st.session_state.pending_clarification = None
    
    # Add user's selection as a message
    st.session_state.messages.append({
        "role": "user",
        "content": f"Selected: {selected_option}"
    })
    
    # Process with clarification
    process_user_query(original_query, clarifications=clarifications)


def render_knowledge_graph_section():
    """Render the Knowledge Graph visualization section"""
    if not st.session_state.kg_loaded or not st.session_state.kg_data:
        st.warning("No Knowledge Graph loaded.")
        if st.button("Go to Database", type="primary"):
            st.session_state.active_section = "database"
            st.rerun()
        return
    
    st.subheader("Knowledge Graph")
    
    tab1, tab2, tab3 = st.tabs(["Graph", "Tables", "JSON"])
    
    kg_data = st.session_state.kg_data
    
    with tab1:
        render_graph_visualization(kg_data)
    
    with tab2:
        render_table_view(kg_data)
    
    with tab3:
        render_json_view(kg_data)


def render_graph_visualization(kg_data: Dict[str, Any]):
    """Render graph visualization"""
    
    if not kg_data or "tables" not in kg_data:
        st.info("No graph data available.")
        return
    
    tables = kg_data.get("tables", {})
    relationships = kg_data.get("relationships", [])
    
    # Stats
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Tables", len(tables))
    with col2:
        total_cols = sum(len(t.get("columns", {})) for t in tables.values())
        st.metric("Columns", total_cols)
    with col3:
        st.metric("Relationships", len(relationships))
    
    st.divider()
    
    # Build nodes and edges
    nodes = []
    edges = []
    
    # Simple color scheme
    colors = ["#2563eb", "#059669", "#d97706", "#dc2626", "#7c3aed", "#0891b2"]
    domain_colors = {}
    
    for i, (table_name, table_info) in enumerate(tables.items()):
        domain = table_info.get("domain", "default")
        if domain not in domain_colors:
            domain_colors[domain] = colors[len(domain_colors) % len(colors)]
        
        nodes.append({
            "id": table_name,
            "label": table_name,
            "color": domain_colors[domain],
            "columns": len(table_info.get("columns", {}))
        })
    
    for rel in relationships:
        edges.append({
            "from": rel.get("from", ""),
            "to": rel.get("to", ""),
            "label": f"{rel.get('from_column', '')} → {rel.get('to_column', '')}"
        })
    
    # Render network
    network_html = create_network_html(nodes, edges)
    st.components.v1.html(network_html, height=450, scrolling=False)
    
    # Legend
    if domain_colors:
        st.markdown("**Domains**")
        cols = st.columns(min(len(domain_colors), 4))
        for i, (domain, color) in enumerate(domain_colors.items()):
            with cols[i % len(cols)]:
                st.markdown(f'<span style="color: {color};">●</span> {domain}', unsafe_allow_html=True)


def create_network_html(nodes: List[Dict], edges: List[Dict]) -> str:
    """Create vis.js network HTML"""
    
    nodes_json = json.dumps(nodes)
    edges_json = json.dumps(edges)
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
        <style>
            #network {{
                width: 100%;
                height: 430px;
                border: 1px solid #e5e7eb;
                border-radius: 8px;
                background: #fafafa;
            }}
        </style>
    </head>
    <body>
        <div id="network"></div>
        <script>
            var nodes = new vis.DataSet({nodes_json}.map(n => ({{
                id: n.id,
                label: n.label + '\\n(' + n.columns + ')',
                color: {{
                    background: n.color,
                    border: n.color,
                    highlight: {{ background: n.color, border: '#1f2937' }}
                }},
                font: {{ color: '#fff', size: 11 }},
                shape: 'box',
                margin: 8
            }})));
            
            var edges = new vis.DataSet({edges_json}.map(e => ({{
                from: e.from,
                to: e.to,
                label: e.label,
                arrows: 'to',
                color: {{ color: '#9ca3af', highlight: '#2563eb' }},
                font: {{ size: 9, color: '#6b7280' }},
                smooth: {{ type: 'cubicBezier' }}
            }})));
            
            var container = document.getElementById('network');
            var data = {{ nodes: nodes, edges: edges }};
            var options = {{
                physics: {{
                    enabled: true,
                    solver: 'forceAtlas2Based',
                    forceAtlas2Based: {{
                        gravitationalConstant: -80,
                        centralGravity: 0.01,
                        springLength: 120,
                        springConstant: 0.08
                    }},
                    stabilization: {{ iterations: 100 }}
                }},
                interaction: {{
                    hover: true,
                    zoomView: true,
                    dragView: true
                }}
            }};
            
            var network = new vis.Network(container, data, options);
        </script>
    </body>
    </html>
    """


def render_table_view(kg_data: Dict[str, Any]):
    """Render table view"""
    
    tables = kg_data.get("tables", {})
    relationships = kg_data.get("relationships", [])
    
    st.markdown("**Tables**")
    
    for table_name, table_info in tables.items():
        with st.expander(f"{table_name}", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                st.caption(f"Domain: {table_info.get('domain', 'N/A')}")
            with col2:
                st.caption(f"Columns: {len(table_info.get('columns', {}))}")
            
            if table_info.get("description"):
                st.markdown(f"*{table_info['description']}*")
            
            columns_data = []
            for col_name, col_info in table_info.get("columns", {}).items():
                columns_data.append({
                    "Column": col_name,
                    "Type": col_info.get("type", "N/A"),
                    "PK": "✓" if col_info.get("pk") else "",
                    "FK": "✓" if col_info.get("fk") else "",
                })
            
            if columns_data:
                st.dataframe(pd.DataFrame(columns_data), use_container_width=True, hide_index=True)
    
    st.markdown("**Relationships**")
    
    if relationships:
        rel_data = [{
            "From": f"{r.get('from', '')}.{r.get('from_column', '')}",
            "To": f"{r.get('to', '')}.{r.get('to_column', '')}"
        } for r in relationships]
        st.dataframe(pd.DataFrame(rel_data), use_container_width=True, hide_index=True)
    else:
        st.info("No relationships")


def render_json_view(kg_data: Dict[str, Any]):
    """Render JSON view"""
    
    json_str = json.dumps(kg_data, indent=2, default=str)
    
    st.download_button(
        "Download JSON",
        data=json_str,
        file_name="knowledge_graph.json",
        mime="application/json"
    )
    
    st.json(kg_data)


def render_history_section():
    """Render query history"""
    st.subheader("Query History")
    
    if not st.session_state.messages:
        st.info("No queries yet.")
        return
    
    queries = [m for m in st.session_state.messages if m["role"] == "user"]
    responses = [m for m in st.session_state.messages if m["role"] == "assistant"]
    
    for i, (q, r) in enumerate(zip(queries, responses)):
        with st.expander(f"Query {i+1}: {q['content'][:40]}...", expanded=False):
            st.markdown(f"**Query:** {q['content']}")
            
            if r.get("sql"):
                st.code(r["sql"], language="sql")
            
            if r.get("data"):
                st.caption(f"{len(r['data'])} rows returned")
                st.dataframe(pd.DataFrame(r["data"]), use_container_width=True, hide_index=True)
            
            if r.get("error"):
                st.error(r["error"])


def main():
    """Main entry point"""
    load_custom_css()
    init_session_state()
    render_header()
    render_sidebar()
    
    section = st.session_state.active_section
    
    if section == "database":
        render_database_section()
    elif section == "chat":
        render_chat_section()
    elif section == "knowledge_graph":
        render_knowledge_graph_section()
    elif section == "history":
        render_history_section()
    else:
        render_database_section()


if __name__ == "__main__":
    main()