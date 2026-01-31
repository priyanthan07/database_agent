import logging
from typing import Dict, Any, Optional
from abc import ABC, abstractmethod

from ..kg.manager.kg_manager import KGManager
from ..openai_client import OpenAIClient
from ..orchestration.agent_state import AgentState

logger = logging.getLogger(__name__)

class BaseAgent(ABC):
    """Base class for all agents with common functionality"""
    
    def __init__(
        self,
        kg_manager: KGManager,
        openai_client: OpenAIClient,
        source_db_conn,
        agent_name: str
    ):
        self.kg_manager = kg_manager
        self.openai_client = openai_client
        self.source_db_conn = source_db_conn
        self.agent_name = agent_name
        self.logger = logging.getLogger(f"{__name__}.{agent_name}")
        
    @abstractmethod
    def process(self, state: AgentState) -> AgentState:
        """
            Main processing method that each agent must implement.
            Takes current state, processes it, and returns updated state.
        """
        pass
    
    def log_start(self, state: AgentState):
        """Log agent start"""
        self.logger.info(f"{self.agent_name} - Starting")
        self.logger.info(f"User Query: {state.user_query}")
        self.logger.info(f"Retry Count: {state.retry_count}")
        
    def log_end(self, state: AgentState, success: bool):
        """Log agent completion"""
        status = "SUCCESS" if success else "FAILED"
        self.logger.info(f"{self.agent_name} - {status}")

    def get_kg(self, kg_id):
        """Load Knowledge Graph"""
        kg = self.kg_manager.load_kg(kg_id)
        if not kg:
            raise ValueError(f"Knowledge Graph not found: {kg_id}")
        return kg
    
    def record_error(self, state: AgentState, error_category: str, error_message: str):
        """Record error in state"""
        error_record = {
            "agent": self.agent_name,
            "error_category": error_category,
            "error_message": error_message,
            "retry_count": state.retry_count
        }
        state.error_history.append(error_record)
        state.error_category = error_category
        state.error_message = error_message
        