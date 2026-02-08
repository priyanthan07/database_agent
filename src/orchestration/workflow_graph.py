import logging
from typing import Dict, Any
from langgraph.graph import StateGraph, END
from langfuse import observe
from langfuse import Langfuse

from .agent_state import AgentState
from ..agents.schema_selector_agent import SchemaSelectorAgent
from ..agents.sql_generator_agent import SQLGeneratorAgent
from ..agents.executor_validator_agent import ExecutorValidatorAgent
from ..agents.tools.clarification_tool import ClarificationTool
from config.settings import Settings


logger = logging.getLogger(__name__)


class AgentWorkflow:
    """
        LangGraph workflow orchestrating the 3-agent system.
    """
    def __init__(
        self,
        kg_manager,
        openai_client,
        source_db_conn,
        memory_repository,
        error_summary_manager=None
    ):
        self.kg_manager = kg_manager
        self.openai_client = openai_client
        self.source_db_conn = source_db_conn
        self.memory_repository = memory_repository
        self.error_summary_manager = error_summary_manager
        
        self.setting = Settings()
        
        self.langfuse = Langfuse(
            public_key=self.setting.LANGFUSE_PUBLIC_KEY,
            secret_key=self.setting.LANGFUSE_SECRET_KEY,
            host=self.setting.LANGFUSE_HOST
        )
        
        # Initialize agents
        self.agent_1 = SchemaSelectorAgent(
            kg_manager=kg_manager,
            openai_client=openai_client,
            source_db_conn=source_db_conn
        )
        
        self.agent_2 = SQLGeneratorAgent(
            kg_manager=kg_manager,
            openai_client=openai_client,
            source_db_conn=source_db_conn,
            memory_repository=memory_repository
        )
        
        self.agent_3 = ExecutorValidatorAgent(
            kg_manager=kg_manager,
            openai_client=openai_client,
            source_db_conn=source_db_conn,
            memory_repository=memory_repository,
            error_summary_manager=error_summary_manager
        )
        
        # Initialize clarification tool for Phase B
        self.clarification_tool = ClarificationTool(openai_client)
        
        # Build workflow graph
        self.graph = self._build_graph()
        
    def _build_graph(self) -> StateGraph:
        """Build LangGraph workflow"""
        
        # Create graph
        workflow = StateGraph(AgentState)
        
        # Add nodes (agents)
        workflow.add_node("agent_1", self._run_agent_1)
        workflow.add_node("phase_b_check", self._run_phase_b_check)
        workflow.add_node("agent_2", self._run_agent_2)
        workflow.add_node("agent_3", self._run_agent_3)
        
        # Add edges
        # Start always goes to Agent 1
        workflow.set_entry_point("agent_1")
        
        # Agent 1 → Phase B Check
        workflow.add_edge("agent_1", "phase_b_check")
        
        # Phase B Check → Agent 2 OR halt (clarification needed)
        workflow.add_conditional_edges(
            "phase_b_check",
            self._phase_b_routing_decision,
            {
                "agent_2": "agent_2",
                "clarification_needed": END,  # Halt workflow, return to user
                "complete": END,  # Agent 1 failed
            }
        )
        
        # Agent 2 → Agent 3
        workflow.add_edge("agent_2", "agent_3")
        
        # Agent 3 → Decision (retry or complete)
        workflow.add_conditional_edges(
            "agent_3",
            self._routing_decision,
            {
                "agent_1": "agent_1",
                "agent_2": "agent_2",
                "complete": END
            }
        )
        
        return workflow.compile()
    
    @observe(name="workflow_node_agent_1", as_type="span")
    def _run_agent_1(self, state: AgentState) -> AgentState:
        """Execute Agent 1 (Schema Selector)"""
        logger.info("Executing Agent 1: Schema Selector")
        return self.agent_1.process(state)
    
    @observe(name="workflow_node_phase_b_check", as_type="span")
    def _run_phase_b_check(self, state: AgentState) -> AgentState:
        """
            Execute Phase B: Schema-aware clarification check.
        """
        logger.info("Executing Phase B: Schema-aware clarification check")
        
        # Skip Phase B if Agent 1 failed (no tables selected)
        if not state.final_tables or not state.table_contexts:
            logger.info("Phase B: Skipping — no tables selected by Agent 1")
            return state
        
        # Skip Phase B if this is an error retry (Agent 3 routed back)
        # On retries, error_retry_check is handled separately in Agent 3's routing
        if state.retry_count > 0:
            logger.info("Phase B: Skipping — this is a retry, not first pass")
            return state
        
        if state.clarifications_provided:
            logger.info("Phase B: Skipping — user already provided clarification response")
            return state
        
        try:
            phase_b_result = self.clarification_tool.phase_b_schema_validation(
                user_query=state.user_query,
                table_contexts=state.table_contexts,
                final_tables=state.final_tables,
                refined_query=state.refined_query,
            )
            
            # Apply auto-resolutions to refined query if any
            if phase_b_result.get("refined_query"):
                logger.info(f"Phase B: Auto-resolutions applied to query")
                state.refined_query = phase_b_result["refined_query"]
            
            if phase_b_result.get("auto_resolutions"):
                logger.info(f"Phase B: {len(phase_b_result['auto_resolutions'])} auto-resolutions applied")
            
            # Check if user input needed
            if phase_b_result.get("needs_clarification"):
                logger.info("Phase B: Clarification needed — halting workflow")
                state.needs_schema_clarification = True
                state.schema_clarification_request = phase_b_result["clarification_request"]
                state.route_to_agent = "clarification_needed"
            else:
                logger.info("Phase B: All clear — proceeding to Agent 2")
            
        except Exception as e:
            logger.error(f"Phase B check failed: {e}", exc_info=True)
            # On failure, don't block — proceed to Agent 2
            logger.info("Phase B: Failed, proceeding without clarification")
        
        return state
    
    def _phase_b_routing_decision(self, state: AgentState) -> str:
        """Route after Phase B check"""
        if isinstance(state, dict):
            needs_clarification = state.needs_schema_clarification
            route = state.route_to_agent
        else:
            needs_clarification = state.needs_schema_clarification
            route = state.route_to_agent
        
        if needs_clarification:
            logger.info("Phase B routing: clarification_needed — halting workflow")
            return "clarification_needed"
        
        # Check if Agent 1 failed
        if route == "complete":
            logger.info("Phase B routing: Agent 1 failed — completing")
            return "complete"
        
        logger.info("Phase B routing: proceeding to Agent 2")
        return "agent_2"
    
    @observe(name="workflow_node_agent_2", as_type="span")
    def _run_agent_2(self, state: AgentState) -> AgentState:
        """Execute Agent 2 (SQL Generator)"""
        logger.info("Executing Agent 2: SQL Generator")
        return self.agent_2.process(state)
    
    @observe(name="workflow_node_agent_3", as_type="span")
    def _run_agent_3(self, state: AgentState) -> AgentState:
        """Execute Agent 3 (Executor & Validator)"""
        logger.info("Executing Agent 3: Executor & Validator")
        return self.agent_3.process(state)
    
    def _routing_decision(self, state: AgentState) -> str:
        """
            Determine next step after Agent 3.
        """
        if isinstance(state, dict):
            route = state.route_to_agent
        else:
            route = state.route_to_agent
        
        logger.info(f"Routing decision: {route}")
        
        if route == "agent_1":
            logger.info("Routing back to Agent 1 for schema re-selection")
            return "agent_1"
        elif route == "agent_2":
            logger.info("Routing back to Agent 2 for SQL re-generation")
            return "agent_2"
        else:
            logger.info("Workflow complete")
            return "complete"
    
    @observe(
        name="langgraph_workflow_execute",
        as_type="span"
    )
    def execute(self, initial_state: AgentState) -> AgentState:
        """
            Execute the complete workflow.
        """
        
        self.langfuse.update_current_span(
            input={
                "user_query": initial_state.user_query,
                "kg_id": str(initial_state.kg_id),
                "has_clarifications": bool(initial_state.clarifications_provided)
            }
        )
        
        logger.info("STARTING AGENT WORKFLOW")
        logger.info(f"User Query: {initial_state.user_query}")
        logger.info(f"KG ID: {initial_state.kg_id}")
        
        try:
            result = self.graph.invoke(initial_state)
            
            if isinstance(result, dict):
                logger.info("Converting dict result to AgentState")
                final_state = AgentState(**result)
            else:
                final_state = result
                
            # Check if workflow halted for clarification
            if final_state.needs_schema_clarification:
                logger.info("WORKFLOW PAUSED — awaiting user clarification")
                self.langfuse.update_current_span(
                    output={
                        "workflow_paused": True,
                        "reason": "schema_clarification_needed"
                    }
                )
                return final_state
            
            logger.info("WORKFLOW COMPLETED")
            logger.info(f"Success: {final_state.execution_success}")
            logger.info(f"Iterations: {final_state.retry_count + 1}")
            
            if final_state.is_retry_success:
                logger.info("Note: Success was achieved after retry (lesson extracted)")
            
            self.langfuse.update_current_span(
                output={
                    "execution_success": final_state.execution_success,
                    "retry_count": final_state.retry_count,
                    "final_route": final_state.route_to_agent
                },
                metadata={
                    "total_time_ms": final_state.total_time_ms,
                    "iterations": final_state.retry_count + 1,
                    "is_retry_success": final_state.is_retry_success
                }
            )
            
            return final_state
            
        except Exception as e:
            logger.error(f"Workflow execution failed: {e}", exc_info=True)
            initial_state.error_message = str(e)
            initial_state.execution_success = False
            return initial_state
        