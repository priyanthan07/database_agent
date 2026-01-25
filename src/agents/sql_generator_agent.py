import logging
import time
from typing import Dict, Any
from pydantic import BaseModel, Field

from .base_agent import BaseAgent
from .tools.query_memory_tool import QueryMemoryTool
from .tools.sql_validation_tool import SQLValidationTool
from ..orchestration.agent_state import AgentState

logger = logging.getLogger(__name__)

class SQLGenerationOutput(BaseModel):
    """Structured output for SQL generation"""
    reasoning: str = Field(description="Chain-of-thought reasoning for SQL generation")
    sql_query: str = Field(description="Generated SQL query")
    explanation: str = Field(description="Explanation of what the query does")
    confidence: float = Field(description="Confidence score 0.0-1.0")

class SQLGeneratorAgent(BaseAgent):
    """
        Agent 2: SQL Generation
    """
    
    def __init__(self, kg_manager, openai_client, source_db_conn, memory_repository):
        super().__init__(
            kg_manager=kg_manager,
            openai_client=openai_client,
            source_db_conn=source_db_conn,
            agent_name="SQL Generator Agent"
        )
        
        # Initialize tools
        self.query_memory = QueryMemoryTool(memory_repository, openai_client)
        self.sql_validator = SQLValidationTool()
        
    def process(self, state: AgentState) -> AgentState:
        """Main processing logic for SQL generation"""
        self.log_start(state)
        start_time = time.time()
        
        try:
            # Step 1: Retrieve similar past queries for few-shot learning
            self.logger.info("Step 1: Retrieving similar past queries")
            similar_queries = self.query_memory.get_similar_queries(
                kg_id=str(state.kg_id),
                state=state,
                limit=5,
                only_successful=True
            )
            state.similar_past_queries = similar_queries
            
            if similar_queries:
                self.logger.info(f"Found {len(similar_queries)} similar past queries")
            
            
            # Step 2: Get SQL lessons from state
            self.logger.info("Step 2: Loading SQL lessons from error summary")
            sql_lessons = state.sql_lessons or ""
            
            if sql_lessons:
                self.logger.info(f"Using SQL lessons from error summary")
            else:
                self.logger.info("No SQL lessons available")
            
            # Step 3: Generate SQL with LLM
            self.logger.info("Step 3: Generating SQL query")
            sql_result = self._generate_sql_with_llm(
                state=state,
                similar_queries=similar_queries,
                sql_lessons=sql_lessons
            )
            
            state.generated_sql = sql_result["sql_query"]
            state.sql_explanation = sql_result["explanation"]
            state.confidence_score = sql_result["confidence"]
            state.generation_reasoning = sql_result["reasoning"]
            
            # Step 4: Validate syntax
            self.logger.info("Step 4: Validating SQL syntax")
            validation_result = self.sql_validator.validate_sql(
                sql=state.generated_sql,
                expected_tables=state.final_tables,
                kg_context=state.table_contexts
            )
            
            if not validation_result["is_valid"]:
                self.logger.warning(f"SQL validation failed: {validation_result['errors']}")
                
                # Attempt self-correction
                self.logger.info("Attempting self-correction...")
                state = self._self_correct_sql(state, validation_result, sql_lessons)

            else:
                self.logger.info("SQL validation passed")
                state.route_to_agent = "agent_3"
            
            # Record timing
            state.sql_generation_time_ms = int((time.time() - start_time) * 1000)
            
            self.log_end(state, success=True)
            return state
            
        except Exception as e:
            self.logger.error(f"SQL generation failed: {e}", exc_info=True)
            self.record_error(state, "sql_generation_error", str(e))
            state.route_to_agent = "complete"
            self.log_end(state, success=False)
            return state
        
    def _generate_sql_with_llm(
        self,
        state: AgentState,
        similar_queries: list,
        sql_lessons: str
    ) -> Dict[str, Any]:
        """Generate SQL using LLM with context"""
        
        # Prepare context for LLM
        query = state.refined_query if state.refined_query else state.user_query
        
        # Format few-shot examples
        examples_text = self.query_memory.format_examples_for_prompt(similar_queries)
        
        # Format table schemas
        schema_text = self._format_table_schemas(state.table_contexts, state.final_tables)
        
        lessons_section = ""
        if sql_lessons and sql_lessons.strip():
            lessons_section = f"""
                IMPORTANT - Learned Rules from Past Mistakes:
                {sql_lessons}

                Apply these rules when generating SQL. These rules were derived from previous errors and their successful fixes.
            """
        
        # Build prompt
        prompt = f"""You are a PostgreSQL expert. Generate a SQL query to answer the user's question.

                    User Question: "{query}"

                    Available Tables and Schema:
                    {schema_text}

                    Similar Past Queries (for reference):
                    {examples_text}

                    {lessons_section}

                    Instructions:
                    1. Think step-by-step using chain-of-thought reasoning
                    2. Use proper JOINs based on the relationships provided
                    3. Include appropriate WHERE clauses for filtering
                    4. Use qualified column names (table.column) to avoid ambiguity
                    5. Consider the data types and sample values shown
                    6. Be mindful of PII columns
                    7. DO NOT use semicolon at the end
                    8. DO NOT include markdown formatting (no ```sql```)

                    Provide:
                    - Chain-of-thought reasoning
                    - The SQL query
                    - Explanation of what it does
                    - Confidence score (0.0-1.0)
                    
                    CRITICAL - You MUST use ALL tables provided in the schema.
                    When a table has foreign keys to other included tables, JOIN them and SELECT 
                    human-readable names instead of IDs:

                    - If order_items.product_id exists AND products table is in schema:
                    JOIN products ON order_items.product_id = products.product_id
                    SELECT products.name AS product_name (NOT just product_id)

                    WRONG: SELECT order_items.product_id FROM order_items
                    CORRECT: SELECT products.name AS product_name FROM order_items JOIN products ON ...

            """

        try:
            result = self.openai_client.generate_structured_completion(
                messages=[
                    {
                        "role": "system",
                        "content": "You are a PostgreSQL expert. Generate accurate SQL queries with clear reasoning."
                    },
                    {"role": "user", "content": prompt}
                ],
                response_model=SQLGenerationOutput,
                model="gpt-4o", 
                temperature=0.0
            )
            
            self.logger.info(f"Generated SQL:\n{result.sql_query}")
            self.logger.info(f"Confidence: {result.confidence}")
            
            return {
                "sql_query": result.sql_query.strip(),
                "explanation": result.explanation,
                "confidence": result.confidence,
                "reasoning": result.reasoning
            }
            
        except Exception as e:
            self.logger.error(f"LLM SQL generation failed: {e}")
            raise
        
    def _self_correct_sql(self, state: AgentState, validation_result: Dict, sql_lessons: str) -> AgentState:
        """Attempt to self-correct SQL based on validation errors"""
        self.logger.info("Self-correcting SQL...")
        
        errors_text = "\n".join(validation_result["errors"])
        
        lessons_section = ""
        if sql_lessons and sql_lessons.strip():
            lessons_section = f"""
                Learned Rules to Apply:
                {sql_lessons}
            """
        
        correction_prompt = f"""The SQL query has validation errors. Please fix them.

                    Original Query:
                    {state.generated_sql}

                    Validation Errors:
                    {errors_text}

                    Tables and Schema:
                    {self._format_table_schemas(state.table_contexts, state.final_tables)}

                    {lessons_section}
                    
                    Generate a corrected SQL query that fixes these errors.
            """

        try:
            result = self.openai_client.generate_structured_completion(
                messages=[
                    {
                        "role": "system",
                        "content": "You are a PostgreSQL expert fixing SQL errors."
                    },
                    {"role": "user", "content": correction_prompt}
                ],
                response_model=SQLGenerationOutput,
                model="gpt-4o",
                temperature=0.0
            )
            
            state.generated_sql = result.sql_query.strip()
            state.correction_summary = f"Self-corrected: {errors_text}"
            
            self.logger.info(f"Corrected SQL:\n{state.generated_sql}")
            
            # Validate again
            new_validation = self.sql_validator.validate_sql(
                sql=state.generated_sql,
                expected_tables=state.final_tables,
                kg_context=state.table_contexts
            )
            
            if new_validation["is_valid"]:
                self.logger.info("Self-correction successful")
                state.route_to_agent = "agent_3"
            else:
                self.logger.warning("Self-correction failed, passing to Agent 3")
                self.record_error(
                    state,
                    "sql_syntax_error",
                    f"Self-correction failed: {new_validation['errors']}"
                )
                state.route_to_agent = "agent_3"
            
            return state
            
        except Exception as e:
            self.logger.error(f"Self-correction failed: {e}")
            state.route_to_agent = "agent_3"
            return state
        
    def _format_table_schemas(self, table_contexts: Dict, table_names: list) -> str:
        """Format table schemas for prompt"""
        lines = []
        
        for table_name in table_names:
            if table_name not in table_contexts:
                continue
            
            context = table_contexts[table_name]
            
            lines.append(f"\nTable: {table_name}")
            if context.get("description"):
                lines.append(f"Description: {context['description']}")
            
            lines.append("Columns:")
            for col_name, col_data in context["columns"].items():
                col_line = f"  - {col_name} ({col_data['data_type']})"
                
                if col_data["is_primary_key"]:
                    col_line += " [PRIMARY KEY]"
                if col_data["is_foreign_key"]:
                    col_line += " [FOREIGN KEY]"
                if col_data["is_pii"]:
                    col_line += " [PII]"
                
                if col_data.get("description"):
                    col_line += f" - {col_data['description']}"
                
                # Add sample values for enum columns
                if col_data.get("enum_values"):
                    samples = ", ".join(col_data["enum_values"][:5])
                    col_line += f" (values: {samples})"
                
                lines.append(col_line)
            
            # Add relationships
            if context.get("relationships"):
                lines.append("Relationships:")
                for rel in context["relationships"]:
                    lines.append(f"  - {rel['join_condition']} ({rel['type']})")
        
        return "\n".join(lines)
    