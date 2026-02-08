import logging
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime
from ...openai_client import OpenAIClient
from ...orchestration.agent_state import ClarificationRequest

logger = logging.getLogger(__name__)

class PhaseAOutput(BaseModel):
    """Phase A: Pre-schema intent ambiguity check"""
    has_ambiguity: bool = Field(description="True ONLY if the query is so vague that table selection cannot even begin")
    ambiguity_description: str = Field(default="", description="What exactly is ambiguous about the intent")
    possible_interpretations: List[str] = Field(default_factory=list,description="2-3 completely different interpretations if ambiguous")
    reasoning: str = Field(description="Why this is or is not ambiguous")

class SchemaMismatchItem(BaseModel):
    """A single mismatch between user query and available schema"""
    user_term: str = Field(description="The term/entity the user referenced")
    issue_type: str = Field(description="Type: 'not_found' | 'multiple_matches' | 'computed_metric' | 'wrong_entity'")
    matched_candidates: List[str] = Field(default_factory=list, description="Possible column/table matches found in schema (empty if none)")
    can_auto_resolve: bool = Field(description="True if there is ONE clear best match the system can use")
    auto_resolution: Optional[str] = Field(default=None, description="The best-guess resolution if can_auto_resolve is True")
    auto_resolution_explanation: Optional[str] = Field(default=None, description="Brief explanation of why this resolution was chosen")

class PhaseBOutput(BaseModel):
    """Phase B: Schema-aware validation result"""
    all_terms_resolved: bool = Field(description="True if every user-referenced entity maps clearly to schema")
    mismatches: List[SchemaMismatchItem] = Field(default_factory=list, description="List of terms that could not be clearly resolved")
    auto_resolutions_applied: List[str] = Field(default_factory=list,description="List of resolutions the system applied automatically")
    needs_user_input: bool = Field(description="True only if there are mismatches that cannot be auto-resolved")
    reasoning: str = Field(description="Overall reasoning about the validation")
    
class ClarificationQuestionOutput(BaseModel):
    """Generated clarification question for unresolvable mismatches"""
    clarification_type: str = Field(description="'mcq' | 'yes_no' | 'suggestion' | 'open_text'")
    question: str = Field(description="Clear, specific question for the user")
    options: List[str] = Field(default_factory=list ,description="Options if mcq type")
    suggested_action: Optional[str] = Field(default=None, description="For suggestion type: what the system will do if user doesn't object")
    proposed_interpretation: Optional[str] = Field(default=None, description="For yes_no type: the interpretation being proposed")

class ErrorRetryClarificationOutput(BaseModel):
    """Clarification check during error retry"""
    needs_clarification: bool = Field(description="True only if the error suggests a genuine ambiguity with multiple valid alternatives")
    clarification_type: str = Field(default="suggestion", description="'mcq' | 'yes_no' | 'suggestion'")
    question: str = Field(default="", description="Question for user if needs_clarification is True")
    options: List[str] = Field(default_factory=list, description="Options if mcq")
    suggested_action: Optional[str] = Field(default=None,description="What the system will try next if user doesn't respond")
    reasoning: str = Field(description="Why clarification is or is not needed")

    
class ClarificationTool:
    """
        Two-phase clarification tool.
    
        Phase A (pre-schema): Lightweight intent check. Fires very rarely — only when
        the query is so vague that table selection cannot begin.
        
        Phase B (post-schema): Schema-aware validation. Checks user-referenced entities
        against actual KG schema. Attempts auto-resolution first, only asks user when
        there are genuinely multiple valid alternatives.
        
        Error retry: When Agent 3 routes back, checks if the error indicates user
        intent was misunderstood (not just a SQL bug).
    """
    
    def __init__(self, openai_client: OpenAIClient):
        self.openai_client = openai_client
        
    def phase_a_intent_check(self, user_query: str) -> Dict[str, Any]:
        """
            Lightweight check BEFORE schema selection.
            Only flags queries that are too vague to even start table selection.
        """
        logger.info(f"Phase A: Intent check for query: '{user_query}'")

        prompt = f"""Analyze this database query to determine if it is clear enough to begin searching for relevant tables.

            User Query: "{user_query}"

            You should mark has_ambiguity = true ONLY if:
            - The query is so vague that you cannot determine what KIND of data is being requested
            (e.g., "show me performance" — performance of what? products? employees? servers?)
            - The query could lead to COMPLETELY DIFFERENT database domains

            You should mark has_ambiguity = false for:
            - Queries with vague time references like "last month", "recently" — these are NOT ambiguous, 
            they have standard interpretations
            - Queries asking for "top" items — assume reasonable defaults (by count, revenue, etc.)
            - Queries where the intent is clear even if details are loose
            (e.g., "show me customer orders" is clear — it's about customers and orders)
            - Any query where a reasonable database expert would know which tables to look at

            Be VERY conservative. 95%+ of queries should pass through without ambiguity.
        """

        try:
            result = self.openai_client.generate_structured_completion(
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a database expert. Only flag queries as ambiguous if "
                            "they are genuinely too vague to determine what data domain is needed. "
                            "Err heavily on the side of NOT flagging."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                response_model=PhaseAOutput,
                model="gpt-4o-mini",
                temperature=0.0,
            )

            if result.has_ambiguity and len(result.possible_interpretations) >= 2:
                logger.info(f"Phase A: Ambiguity detected — {result.ambiguity_description}")

                clarification = self._build_phase_a_clarification(
                    user_query, result
                )
                return {
                    "needs_clarification": True,
                    "clarification_request": clarification,
                }

            logger.info(f"Phase A: Query is clear — {result.reasoning}")
            return {"needs_clarification": False}

        except Exception as e:
            logger.error(f"Phase A failed: {e}")
            # On failure, don't block — proceed without clarification
            return {"needs_clarification": False}    
    
    def _build_phase_a_clarification(self, user_query: str, result: PhaseAOutput) -> ClarificationRequest:
        """Build clarification request from Phase A result"""
        
        if len(result.possible_interpretations) <= 4:
            return ClarificationRequest(
                clarification_type="mcq",
                question=f"Your query could mean different things. What are you looking for?",
                options=result.possible_interpretations,
                detected_ambiguity=result.ambiguity_description,
                trigger_phase="pre_schema",
            )
        else:
            return ClarificationRequest(
                clarification_type="open_text",
                question=f"Could you be more specific? {result.ambiguity_description}",
                detected_ambiguity=result.ambiguity_description,
                trigger_phase="pre_schema",
            )
            
    def phase_b_schema_validation(
        self,
        user_query: str,
        table_contexts: Dict[str, Dict],
        final_tables: List[str],
        refined_query: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
            Schema-aware validation AFTER Agent 1 selects tables.
            
            Checks user-referenced entities against actual schema.
            Attempts auto-resolution first. Only asks user when genuinely unresolvable.
        """
        query = refined_query if refined_query else user_query
        logger.info(f"Phase B: Schema validation for query: '{query}'")
        logger.info(f"Phase B: Validating against tables: {final_tables}")

        # Build schema summary for LLM
        schema_summary = self._build_schema_summary(table_contexts, final_tables)

        prompt = f"""You are validating a user's database query against the actual available schema.

                User Query: "{query}"

                Available Schema:
                {schema_summary}

                Your task:
                1. Identify every entity, column, metric, or concept the user references in their query
                2. For EACH one, check if it clearly maps to something in the schema above
                3. If a term doesn't map directly, check if:
                a) There's ONE obvious match (different naming) → mark can_auto_resolve = true
                b) There are MULTIPLE plausible matches → mark can_auto_resolve = false  
                c) There's NO match at all → mark can_auto_resolve = false

                Auto-resolution guidelines:
                - "revenue" → if there's a "total_amount" or "price * quantity" available, auto-resolve
                - "customer name" → if there's "first_name"/"last_name" or "customer_name", auto-resolve  
                - Singular/plural differences → auto-resolve
                - Common synonyms with ONE clear match → auto-resolve

                Do NOT flag as mismatch:
                - Standard SQL concepts (COUNT, SUM, AVG, etc.)
                - Time references ("last month", "this year") — these are NOT schema issues
                - Ordering/limiting ("top 10", "highest") — these are SQL operations, not entities
                - Terms that clearly map to available columns even with slightly different wording

                Only set needs_user_input = true if there are mismatches where can_auto_resolve = false 
                AND the mismatch would lead to an incorrect query.
            """

        try:
            result = self.openai_client.generate_structured_completion(
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a database expert validating query terms against schema. "
                            "Auto-resolve when possible. Only flag genuine unresolvable mismatches."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                response_model=PhaseBOutput,
                model="gpt-4o",
                temperature=0.0,
            )

            # Log auto-resolutions
            if result.auto_resolutions_applied:
                for resolution in result.auto_resolutions_applied:
                    logger.info(f"Phase B: Auto-resolved: {resolution}")

            # If everything resolved, proceed
            if result.all_terms_resolved or not result.needs_user_input:
                logger.info(f"Phase B: All terms resolved — {result.reasoning}")
                
                # Build updated query hint from auto-resolutions
                updated_query = self._apply_auto_resolutions(
                    query, result.auto_resolutions_applied, result.mismatches
                )
                
                return {
                    "needs_clarification": False,
                    "auto_resolutions": result.auto_resolutions_applied,
                    "refined_query": updated_query if updated_query != query else None,
                }

            # There are unresolvable mismatches — need user input
            unresolvable = [m for m in result.mismatches if not m.can_auto_resolve]
            
            # Split: terms with NO candidates should be auto-resolved (dropped with a note),
            # only terms with MULTIPLE candidates need user input
            no_match_terms = [m for m in unresolvable if not m.matched_candidates]
            multi_match_terms = [m for m in unresolvable if m.matched_candidates]
            
            logger.info(f"Phase B: {len(unresolvable)} unresolvable mismatches found")
            logger.info(f"Phase B: {len(no_match_terms)} no-match terms (will auto-resolve), {len(multi_match_terms)} multi-match terms (need user input)")

            # Auto-resolve no-match terms by adding "not available" hints
            no_match_hints = []
            for m in no_match_terms:
                hint = f'"{m.user_term}" has no match in the database schema — omit from results'
                no_match_hints.append(hint)
                logger.info(f"Phase B: Auto-dropping unmatched term: '{m.user_term}'")

            # Apply no-match hints to the query
            updated_query = query
            if no_match_hints:
                all_hints = list(result.auto_resolutions_applied or []) + no_match_hints
                updated_query = query + " [Context: " + "; ".join(no_match_hints) + "]"
                
            # Only ask user if there are terms with multiple valid candidates
            if multi_match_terms:
                logger.info(f"Phase B: {len(multi_match_terms)} terms need user clarification")
                
                clarification = self._build_phase_b_clarification(
                    query, multi_match_terms, table_contexts
                )
                
                return {
                    "needs_clarification": True,
                    "clarification_request": clarification,
                    "auto_resolutions": (result.auto_resolutions_applied or []) + no_match_hints,
                    "refined_query": updated_query if updated_query != query else None,
                }
            else:
                # All unresolvable terms were no-match — auto-resolved, proceed
                logger.info("Phase B: All unresolvable terms were no-match — auto-resolved, proceeding")
                return {
                    "needs_clarification": False,
                    "auto_resolutions": (result.auto_resolutions_applied or []) + no_match_hints,
                    "refined_query": updated_query if updated_query != query else None,
                }
                
                
        except Exception as e:
            logger.error(f"Phase B failed: {e}")
            # On failure, don't block — proceed without clarification
            return {"needs_clarification": False, "auto_resolutions": []}
        
    def _build_phase_b_clarification(
        self,
        user_query: str,
        unresolvable: List[SchemaMismatchItem],
        table_contexts: Dict[str, Dict],
    ) -> ClarificationRequest:
        """Build clarification request from unresolvable schema mismatches"""

        # Focus on the most critical mismatch
        primary = unresolvable[0]

        schema_summary = self._build_schema_summary(table_contexts, list(table_contexts.keys()))

        prompt = f"""Generate a clarification question for this unresolved schema mismatch.

                User Query: "{user_query}"

                Unresolved Issue:
                - User term: "{primary.user_term}"
                - Issue type: {primary.issue_type}
                - Candidates found: {primary.matched_candidates if primary.matched_candidates else "None"}

                Available Schema:
                {schema_summary}

                Guidelines:
                - If there are 2-4 specific candidates: use clarification_type = "mcq" with those as options
                - If there is ONE likely candidate but you're not fully sure: use clarification_type = "suggestion"
                and set suggested_action to what the system will do
                - If the term has NO matches at all: use clarification_type = "open_text" asking what they mean
                - If asking yes/no about a single interpretation: use clarification_type = "yes_no"

                Make the question specific and brief. Do not ask unnecessary questions.
        """

        try:
            result = self.openai_client.generate_structured_completion(
                messages=[
                    {
                        "role": "system",
                        "content": "Generate a clear, minimal clarification question. Be specific.",
                    },
                    {"role": "user", "content": prompt},
                ],
                response_model=ClarificationQuestionOutput,
                model="gpt-4o-mini",
                temperature=0.0,
            )

            return ClarificationRequest(
                clarification_type=result.clarification_type,
                question=result.question,
                options=result.options,
                suggested_action=result.suggested_action,
                suggested_interpretation=result.suggested_action,
                proposed_interpretation=result.proposed_interpretation,
                detected_ambiguity=f"{primary.issue_type}: {primary.user_term}",
                trigger_phase="post_schema",
            )

        except Exception as e:
            logger.error(f"Phase B clarification generation failed: {e}")
            # Fallback
            if primary.matched_candidates:
                return ClarificationRequest(
                    clarification_type="mcq",
                    question=f'What do you mean by "{primary.user_term}"?',
                    options=primary.matched_candidates[:4],
                    detected_ambiguity=f"{primary.issue_type}: {primary.user_term}",
                    trigger_phase="post_schema",
                )
            else:
                return ClarificationRequest(
                    clarification_type="open_text",
                    question=f'No match found for "{primary.user_term}" in the database. What specifically are you looking for?',
                    detected_ambiguity=f"not_found: {primary.user_term}",
                    trigger_phase="post_schema",
                )
                
    def error_retry_check(
        self,
        user_query: str,
        error_message: str,
        error_category: str,
        table_contexts: Dict[str, Dict],
        final_tables: List[str],
        error_history: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
            Check if an error retry needs user clarification.
            
            Only triggers if the error suggests the user's intent was misunderstood
            AND there are genuinely multiple valid alternatives.
        """
        logger.info(f"Error retry clarification check: {error_category}")

        schema_summary = self._build_schema_summary(table_contexts, final_tables)

        # Format error history
        history_text = ""
        for i, err in enumerate(error_history[-3:], 1):  # Last 3 errors
            history_text += f"{i}. {err.get('error_category', 'unknown')}: {err.get('error_message', '')[:150]}\n"

        prompt = f"""A SQL query failed. Determine if the error suggests the user's intent was misunderstood 
                and there are multiple valid alternatives that require user input.

                User Query: "{user_query}"
                Error Message: {error_message}
                Error Category: {error_category}

                Recent Error History:
                {history_text}

                Available Schema:
                {schema_summary}

                You should set needs_clarification = true ONLY if:
                - The error is because the system chose the wrong table/column interpretation
                AND there are 2+ genuinely different valid alternatives
                - The same type of error keeps recurring (check history), suggesting a fundamental misunderstanding

                You should set needs_clarification = false if:
                - The error is a SQL syntax/logic bug the system can fix itself
                - There's only ONE reasonable interpretation (system just made a mistake)
                - The error is a timeout, permission, or infrastructure issue
                - The system hasn't tried alternative approaches yet

                If clarification is needed, suggest what the system will try next (suggested_action)
                so it can auto-proceed if the user doesn't respond.
            """

        try:
            result = self.openai_client.generate_structured_completion(
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are analyzing a failed SQL query to determine if user clarification "
                            "is needed. Only request clarification when there are genuinely multiple "
                            "valid interpretations. Most errors are fixable without user input."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                response_model=ErrorRetryClarificationOutput,
                model="gpt-4o-mini",
                temperature=0.0,
            )

            if result.needs_clarification:
                logger.info(f"Error retry: Clarification needed — {result.reasoning}")

                clarification = ClarificationRequest(
                    clarification_type=result.clarification_type,
                    question=result.question,
                    options=result.options,
                    suggested_action=result.suggested_action,
                    suggested_interpretation=result.suggested_action,
                    detected_ambiguity=f"error_retry: {error_category}",
                    trigger_phase="error_retry",
                )

                return {
                    "needs_clarification": True,
                    "clarification_request": clarification,
                }

            logger.info(f"Error retry: No clarification needed — {result.reasoning}")
            return {"needs_clarification": False}

        except Exception as e:
            logger.error(f"Error retry clarification check failed: {e}")
            return {"needs_clarification": False}
        
    def _build_schema_summary(self, table_contexts: Dict[str, Dict], table_names: List[str]) -> str:
        """Build concise schema summary for LLM prompts"""
        lines = []

        for table_name in table_names:
            context = table_contexts.get(table_name)
            if not context:
                continue

            lines.append(f"\nTable: {table_name}")
            if context.get("description"):
                lines.append(f"  Description: {context['description']}")

            columns = context.get("columns", {})
            lines.append(f"  Columns:")
            for col_name, col_data in columns.items():
                parts = [f"    - {col_name} ({col_data.get('data_type', 'unknown')})"]
                if col_data.get("is_primary_key"):
                    parts.append("[PK]")
                if col_data.get("is_foreign_key"):
                    parts.append("[FK]")
                if col_data.get("description"):
                    parts.append(f"— {col_data['description']}")
                lines.append(" ".join(parts))

            relationships = context.get("relationships", [])
            if relationships:
                lines.append(f"  Relationships:")
                for rel in relationships:
                    lines.append(
                        f"    - {rel.get('from_table', '')}.{rel.get('from_column', '')} "
                        f"→ {rel.get('to_table', '')}.{rel.get('to_column', '')}"
                    )

        return "\n".join(lines)
        
    def _apply_auto_resolutions(
        self,
        query: str,
        auto_resolutions: List[str],
        mismatches: List[SchemaMismatchItem],
    ) -> str:
        """
            Apply auto-resolutions to refine the query.
            Appends resolution hints that Agent 2 can use.
        """
        if not auto_resolutions and not any(m.can_auto_resolve for m in mismatches):
            return query

        hints = []
        for mismatch in mismatches:
            if mismatch.can_auto_resolve and mismatch.auto_resolution:
                hints.append(
                    f'"{mismatch.user_term}" refers to {mismatch.auto_resolution}'
                )

        if hints:
            resolution_note = " [Context: " + "; ".join(hints) + "]"
            return query + resolution_note

        return query
        