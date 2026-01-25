"""
Configuration for Text2SQL Streamlit UI
"""

# App Configuration
APP_NAME = "Text2SQL Agent"
APP_VERSION = "1.0.0"
APP_DESCRIPTION = "Transform natural language into SQL queries with AI-powered intelligence"

# Theme Colors
THEME = {
    "primary": "#667eea",
    "secondary": "#764ba2",
    "success": "#10b981",
    "warning": "#f59e0b",
    "error": "#ef4444",
    "info": "#3b82f6",
    "background": "#ffffff",
    "surface": "#f8f9fa",
    "text_primary": "#1f2937",
    "text_secondary": "#6b7280",
}

# Domain Colors for KG Visualization
DOMAIN_COLORS = [
    "#667eea",  # Purple
    "#10b981",  # Green
    "#f59e0b",  # Orange
    "#ef4444",  # Red
    "#06b6d4",  # Cyan
    "#8b5cf6",  # Violet
    "#ec4899",  # Pink
    "#84cc16",  # Lime
]

# Default Connection Settings
DEFAULT_DB_SETTINGS = {
    "host": "localhost",
    "port": 5432,
    "database": "",
    "user": "postgres",
    "password": ""
}

# Chat Configuration
CHAT_CONFIG = {
    "max_history": 100,
    "show_sql_by_default": True,
    "show_explanation_by_default": True,
}

# Progress Steps for KG Building
KG_BUILD_STEPS = [
    {"id": "connect", "label": "Connect"},
    {"id": "extract", "label": "Extract Schema"},
    {"id": "enrich", "label": "Enrich with AI"},
    {"id": "embed", "label": "Generate Embeddings"},
    {"id": "complete", "label": "Complete"},
]