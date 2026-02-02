import logging
import threading
from typing import Dict, Any, Optional, List
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, Field
from psycopg2.extras import RealDictCursor
from langfuse import observe
from langfuse import Langfuse

from config.settings import Settings

logger = logging.getLogger(__name__)

class LessonExtractionOutput(BaseModel):
    lesson_type : str = Field(description="Type of lesson: 'schema' or 'sql'")
    lesson_rule: str = Field(description="Concise rule derived from the error and fix (max 30 words)")
    
class CompressionOutput(BaseModel):
    compressed_lessons: str = Field(description="Compressed lessons text, merging similar rules")

class ErrorSummaryManager:
    """
       Manages error learning summaries for Knowledge Graphs.
       
       Design:
        - Word count threshold: x words triggers compression
        - Compression target: 50% reduction (merge 2-3 rules into 1)
        - Max after compression: ~x/2 words
        - Async compression: Runs in background thread
    """
    
    DEFAULT_COMPRESSION_THRESHOLD = 500  
    
    def __init__(self, kg_conn, openai_client):
        self.conn = kg_conn
        self.openai_client = openai_client
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._compression_lock = threading.Lock()
        
        self.setting = Settings()
        
        self.langfuse = Langfuse(
            public_key=self.setting.LANGFUSE_PUBLIC_KEY,
            secret_key=self.setting.LANGFUSE_SECRET_KEY,
            host=self.setting.LANGFUSE_HOST
        )
    
    @observe(
        name="em_get_error_summary",
        as_type="span"
    )
    def get_summary(self, kg_id: UUID) -> Dict[str, Any]:
        
        kg_id_str = str(kg_id)
        
        # Check cache first
        if kg_id_str in self._cache:
            logger.debug(f"Returning cached summary for KG: {kg_id_str}")
            
            self.langfuse.update_current_span(
                metadata={"cache_hit": True},
                output={
                    "lesson_count": self._cache[kg_id_str].get("lesson_count", 0),
                    "word_count": self._cache[kg_id_str].get("word_count", 0)
                }
            )
            
            return self._cache[kg_id_str]
        
        # Load from database
        summary = self._load_summary_from_db(kg_id)
        
        if summary:
            self._cache[kg_id_str] = summary
            logger.info(f"Loaded error summary for KG: {kg_id_str} ({summary.get('lesson_count', 0)} lessons)")
            
            self.langfuse.update_current_span(
                metadata={"cache_hit": False},
                output={
                    "lesson_count": summary.get("lesson_count", 0),
                    "word_count": summary.get("word_count", 0)
                }
            )
            
        else:
            # Create empty summary if not exists
            summary = self._create_empty_summary(kg_id)
            self._cache[kg_id_str] = summary
            logger.info(f"Created empty error summary for KG: {kg_id_str}")
            
            self.langfuse.update_current_span(
                metadata={"cache_hit": False, "created_new": True},
                output={"lesson_count": 0, "word_count": 0}
            )
            
        return summary
    
    def _load_summary_from_db(self, kg_id: UUID) -> Optional[Dict[str, Any]]:
        
        query = """
            SELECT kg_id, schema_lessons, sql_lessons, lesson_count,
                   word_count, compression_threshold, last_updated, version
            FROM kg_error_summary
            WHERE kg_id = %s
        """
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (str(kg_id),))
                result = cur.fetchone()
                
                if result:
                    return dict(result)
                return None
            
        except Exception as e:
            logger.error(f"Failed to load error summary: {e}")
            return None
        
    def _create_empty_summary(self, kg_id: UUID) -> Dict[str, Any]:
        query = """
            INSERT INTO kg_error_summary (kg_id, schema_lessons, sql_lessons)
            VALUES (%s, '', '')
            ON CONFLICT (kg_id) DO NOTHING
            RETURNING kg_id, schema_lessons, sql_lessons, lesson_count,
                      word_count, compression_threshold, last_updated, version
        """
        try:
            self.conn.rollback()
            
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (str(kg_id),))
                result = cur.fetchone()
                self.conn.commit()
                
                if result:
                    return dict(result)
                
                # If ON CONFLICT triggered, fetch existing
                return self._load_summary_from_db(kg_id) or {
                    "kg_id": kg_id,
                    "schema_lessons": "",
                    "sql_lessons": "",
                    "lesson_count": 0,
                    "word_count": 0,
                    "compression_threshold": self.DEFAULT_COMPRESSION_THRESHOLD,
                    "version": 1
                }
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to create error summary: {e}")
            return {
                "kg_id": kg_id,
                "schema_lessons": "",
                "sql_lessons": "",
                "lesson_count": 0,
                "word_count": 0,
                "compression_threshold": self.DEFAULT_COMPRESSION_THRESHOLD,
                "version": 1
            }
    
    @observe(
        name="em_add_lesson_from_error",
        as_type="span"
    )    
    def add_lesson_from_error(
        self,
        kg_id: UUID,
        error_message: str,
        error_category: str,
        fix_applied: str,
        affected_tables: List[str],
        generated_sql: str
    ) -> bool:
        """
            Extract lesson from error and add to summary.
            Triggers compression if threshold reached.
        """
        
        self.langfuse.update_current_span(
            input={
                "kg_id": str(kg_id),
                "error_category": error_category,
                "affected_tables": affected_tables
            }
        )
        
        logger.info(f"Extracting lesson from error: {error_category}")
        
        try:
            # Extract lesson using LLM
            lesson = self._extract_lesson(
                error_message=error_message,
                error_category=error_category,
                fix_applied=fix_applied,
                affected_tables=affected_tables,
                generated_sql=generated_sql
            )
            
            if not lesson:
                logger.warning("Failed to extract lesson from error")
                return False
            
            # Add lesson to appropriate category
            success = self._add_lesson_to_summary(
                kg_id=kg_id,
                lesson_type=lesson["lesson_type"],
                lesson_rule=lesson["lesson_rule"]
            )
            
            if success:
                logger.info(f"Added {lesson['lesson_type']} lesson: {lesson['lesson_rule'][:50]}...")
                
                self.langfuse.update_current_span(
                    output={
                        "lesson_type": lesson["lesson_type"],
                        "lesson_added": True
                    }
                )
                
                # Check if compression needed (async)
                self._check_and_trigger_compression(kg_id)
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to add lesson from error: {e}")
            return False
    
    @observe(
        name="em_add_lesson_from_feedback",
        as_type="span"
    ) 
    def add_lesson_from_feedback(
        self,
        kg_id: UUID,
        query_log: Dict[str, Any],
        feedback: str,
        rating: Optional[int] = None,
        error_patterns: Optional[List[Dict[str, Any]]] = None
    ) -> bool:
        """
            Extract lesson from user feedback on a query.
        """
        
        self.langfuse.update_current_span(
            input={
                "kg_id": str(kg_id),
                "feedback": feedback[:100],
                "rating": rating,
                "has_error_patterns": bool(error_patterns)
            }
        )
        
        logger.info(f"Extracting lesson from feedback: '{feedback[:50]}...'")
        
        if error_patterns:
            logger.info(f"Using {len(error_patterns)} related error patterns for context")
        
        try:
            # Determine feedback severity
            is_negative = (
                feedback in ["Not helpful", "not_helpful", "incorrect"] or
                (rating is not None and rating <= 2) or
                "wrong" in feedback.lower() or
                "incorrect" in feedback.lower() or
                "bad" in feedback.lower()
            )
            
            execution_success = query_log.get("execution_success", False)
            
            # Only extract lessons from negative feedback or failed queries
            if not is_negative and execution_success:
                logger.info("Positive feedback on successful query - no lesson needed")
                return False
            
            # Extract lesson using LLM
            lesson = self._extract_lesson_from_feedback(
                query_log=query_log,
                feedback=feedback,
                rating=rating,
                error_patterns=error_patterns
            )
            
            if not lesson:
                logger.warning("Failed to extract lesson from feedback")
                return False
            
            # Add lesson to appropriate category
            success = self._add_lesson_to_summary(
                kg_id=kg_id,
                lesson_type=lesson["lesson_type"],
                lesson_rule=lesson["lesson_rule"]
            )
            
            if success:
                logger.info(f"Added {lesson['lesson_type']} lesson from feedback: {lesson['lesson_rule'][:50]}...")

                self.langfuse.update_current_span(
                    output={"lesson_extracted": True}
                )
            
            else:
                logger.info("No lesson extracted from feedback")
                
                # Log output
                self.langfuse.update_current_span(
                    output={"lesson_extracted": False}
                )
                
            return success
            
        except Exception as e:
            logger.error(f"Failed to add lesson from feedback: {e}")
            return False
    
    def _extract_lesson_from_feedback(
        self,
        query_log: Dict[str, Any],
        feedback: str,
        rating: Optional[int] = None,
        error_patterns: Optional[List[Dict[str, Any]]] = None
    ) -> Optional[Dict[str, str]]:
        """
            Use LLM to extract a lesson from user feedback.
            
            Combines query context + execution result + user feedback to derive lesson.
        """
        user_question = query_log.get("user_question", "")
        generated_sql = query_log.get("generated_sql", "")
        execution_success = query_log.get("execution_success", False)
        error_message = query_log.get("error_message", "")
        error_category = query_log.get("error_category", "")
        tables_used = query_log.get("tables_used", [])
        
        # Build context for LLM
        execution_context = ""
        if execution_success:
            execution_context = "Query executed successfully, but user provided negative feedback."
        else:
            execution_context = f"Query failed with error: {error_message}\nError category: {error_category}"
        
        rating_context = f"User rating: {rating}/5" if rating else ""
        
        error_patterns_context = ""
        if error_patterns and len(error_patterns) > 0:
            error_patterns_context = "\n\nRelated Error Patterns from Past Queries:"
            for i, pattern in enumerate(error_patterns, 1):
                error_patterns_context += f"""
                                {i}. Error Pattern:
                                - Category: {pattern.get('error_category')}
                                - Pattern Description: {pattern.get('error_pattern', 'N/A')}
                                - Fix Applied: {pattern.get('fix_applied', 'N/A')}
                                - Occurrence Count: {pattern.get('occurrence_count', 0)}
                                - Success Rate: {pattern.get('success_rate_after_fix', 'N/A')}
                                """
        
        prompt = f"""Analyze this database query and user feedback to extract a reusable lesson.

        User Question: {user_question}

        Generated SQL: {generated_sql}

        Execution Result: {execution_context}

        Tables Used: {', '.join(tables_used) if tables_used else 'None'}

        User Feedback: {feedback}
        {rating_context}
        {error_patterns_context}
        
        IMPORTANT: 
        - If multiple issues exist, identify the PRIMARY ROOT CAUSE
        - Extract ONE lesson for the most critical issue
        - Prioritize schema issues over SQL formatting issues
        - Prioritize logic errors over syntax errors
        - Use error patterns to understand recurring problems and their solutions

        Based on the user's feedback and query context, determine:
        1. lesson_type: Is this a 'schema' lesson (about table/column selection) or 'sql' lesson (about SQL syntax/logic/quality)?
        2. lesson_rule: Write a concise rule (max 30 words) that would prevent this issue in future queries.

        Guidelines for lesson_type:
        - schema: Wrong tables selected, missing columns, incorrect relationships, missing enrichment tables
        - sql: Syntax errors, incorrect joins, wrong aggregations, data type issues, logic errors, result quality issues

        Guidelines for lesson_rule:
        - Focus on the ROOT CAUSE indicated by feedback
        - Make it actionable and specific
        - Format: "When [condition], [action]" or "Always/Never [action] when [condition]"

        Examples:
        - Schema lesson: "When user asks about product names, always include products table for human-readable output, not just product IDs"
        - SQL lesson: "When aggregating data, always include GROUP BY clause for non-aggregated columns"
        """
        
        try:
            result = self.openai_client.generate_structured_completion(
                messages=[
                    {
                        "role": "system",
                        "content": "You are a database expert extracting reusable rules from query feedback. Be concise and specific."
                    },
                    {"role": "user", "content": prompt}
                ],
                response_model=LessonExtractionOutput,
                model="gpt-4o-mini",
                temperature=0.0
            )
            
            logger.info(f"Extracted lesson from feedback: {result.lesson_type} - {result.lesson_rule}")
            
            return {
                "lesson_type": result.lesson_type,
                "lesson_rule": result.lesson_rule
            }
            
        except Exception as e:
            logger.error(f"LLM lesson extraction from feedback failed: {e}")
            return None
        
    def _extract_lesson(
        self,
        error_message: str,
        error_category: str,
        fix_applied: str,
        affected_tables: List[str],
        generated_sql: str
    ) -> Optional[Dict[str, str]]:
        """Use LLM to extract a concise lesson from the error"""
        
        prompt = f"""Analyze this database query error and the fix that resolved it. Extract a concise, reusable rule.

                    Error Category: {error_category}
                    Error Message: {error_message}
                    Affected Tables: {', '.join(affected_tables) if affected_tables else 'None'}
                    Fix Applied: {fix_applied}
                    SQL Context: {generated_sql[:500] if generated_sql else 'N/A'}

                    Determine:
                    1. lesson_type: Is this a 'schema' lesson (about table/column selection) or 'sql' lesson (about SQL syntax/logic)?
                    2. lesson_rule: Write a concise rule (max 30 words) that would prevent this error in future queries.

                    Guidelines for lesson_type:
                    - schema: About which tables to include, column selection, relationships, missing tables
                    - sql: About syntax, joins, aggregations, filters, data types, column references

                    Rule format: "When [condition], [action]" or "Always/Never [action] when [condition]"
                """
        try:
            result = self.openai_client.generate_structured_completion(
                messages=[
                    {
                        "role": "system",
                        "content": "You are a database expert extracting reusable rules from query errors. Be concise and specific."
                    },
                    {"role": "user", "content": prompt}
                ],
                response_model=LessonExtractionOutput,
                model="gpt-4o-mini",
                temperature=0.0
            )
            
            return {
                "lesson_type": result.lesson_type,
                "lesson_rule": result.lesson_rule
            }
            
        except Exception as e:
            logger.error(f"LLM lesson extraction failed: {e}")
            return None
        
    def _add_lesson_to_summary(
        self,
        kg_id: UUID,
        lesson_type: str,
        lesson_rule: str
    ) -> bool:
        """Add lesson to the appropriate summary field"""
        
        # Determine which field to update
        if lesson_type == "schema":
            field = "schema_lessons"
        else:
            field = "sql_lessons"
            
        # Get current summary
        summary = self.get_summary(kg_id)
        current_lessons = summary.get(field, "")
        other_field = "sql_lessons" if field == "schema_lessons" else "schema_lessons"
        other_lessons = summary.get(other_field, "")
        
        # Count current lessons for numbering
        current_count = summary.get("lesson_count", 0)
        new_lesson_num = current_count + 1
        
        # Format new lesson with number
        formatted_lesson = f"{new_lesson_num}. {lesson_rule}"
        
        # Append to existing lessons
        if current_lessons:
            updated_lessons = f"{current_lessons}\n{formatted_lesson}"
        else:
            updated_lessons = formatted_lesson
            
        # Calculate total word count
        new_word_count = len(updated_lessons.split()) + len(other_lessons.split())
        
        # Update database
        query = f"""
            UPDATE kg_error_summary
            SET {field} = %s,
                lesson_count = lesson_count + 1,
                word_count = %s,
                last_updated = CURRENT_TIMESTAMP,
                version = version + 1
            WHERE kg_id = %s
            RETURNING version
        """
        
        try:
            self.conn.rollback()
            
            with self.conn.cursor() as cur:
                cur.execute(query, (updated_lessons, new_word_count, str(kg_id)))
                result = cur.fetchone()
                self.conn.commit()
                
                if result:
                    # Update cache
                    kg_id_str = str(kg_id)
                    if kg_id_str in self._cache:
                        self._cache[kg_id_str][field] = updated_lessons
                        self._cache[kg_id_str]["lesson_count"] = new_lesson_num
                        self._cache[kg_id_str]["word_count"] = new_word_count
                        self._cache[kg_id_str]["version"] = result[0]
                    logger.info(f"Summary updated: {new_lesson_num} lessons, {new_word_count} words")
                    return True
                
            return False
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to add lesson to summary: {e}")
            return False
        
    def _check_and_trigger_compression(self, kg_id: UUID):
        """Check if compression threshold reached and trigger async compression"""
        
        summary = self.get_summary(kg_id)
        word_count = summary.get("word_count", 0)
        threshold = summary.get("compression_threshold", self.DEFAULT_COMPRESSION_THRESHOLD)
        
        if word_count >= threshold:
            logger.info(f"Word count ({word_count}) >= threshold ({threshold}). Triggering async compression.")
            
            # Run compression in background thread
            thread = threading.Thread(
                target=self._compress_summary_async,
                args=(kg_id,),
                daemon=True
            )
            thread.start()
            
    def _compress_summary_async(self, kg_id: UUID):
        """
            Compress summary asynchronously via LLM.
            Target: 50% reduction by merging 2-3 similar rules into one.
        """
        
        # Use lock to prevent concurrent compressions for same KG
        with self._compression_lock:
            logger.info(f"Starting async compression for KG: {kg_id}")
            
            try:
                # Reload from DB to get latest
                summary = self._load_summary_from_db(kg_id)
                if not summary:
                    return

                schema_lessons = summary.get("schema_lessons", "")
                sql_lessons = summary.get("sql_lessons", "")
                threshold = summary.get("compression_threshold", self.DEFAULT_COMPRESSION_THRESHOLD)
                
                # Target: 50% of threshold
                target_words = threshold // 2
                
                # Compress each category
                compressed_schema = ""
                compressed_sql = ""
                
                if schema_lessons:
                    compressed_schema = self._compress_lessons(
                        lessons_text=schema_lessons,
                        lesson_type="schema",
                        target_words=target_words // 2
                    )
                    
                if sql_lessons:
                    compressed_sql = self._compress_lessons(
                        lessons_text=sql_lessons,
                        lesson_type="sql",
                        target_words=target_words // 2
                    )
                    
                # Calculate new metrics
                new_word_count = len(compressed_schema.split()) + len(compressed_sql.split())

                # Count rules after compression
                schema_count = len([l for l in compressed_schema.split('\n') if l.strip()]) if compressed_schema else 0
                sql_count = len([l for l in compressed_sql.split('\n') if l.strip()]) if compressed_sql else 0
                new_lesson_count = schema_count + sql_count
                
                # Save compressed summary
                self._save_compressed_summary(
                    kg_id=kg_id,
                    schema_lessons=compressed_schema,
                    sql_lessons=compressed_sql,
                    lesson_count=new_lesson_count,
                    word_count=new_word_count
                )
                
                # Invalidate cache
                kg_id_str = str(kg_id)
                if kg_id_str in self._cache:
                    del self._cache[kg_id_str]
                
                logger.info(f"Compression complete. Before: {summary.get('word_count', 0)} words, After: {new_word_count} words")
                
            except Exception as e:
                logger.error(f"Async compression failed: {e}")
                
    def _compress_lessons(self, lessons_text: str, lesson_type: str, target_words: int) -> str:
        """
            Use LLM to compress lessons by merging similar rules.
            Target: 50% reduction by merging 2-3 rules into one general rule.
        """
        
        if not lessons_text.strip():
            return ""
        
        current_words = len(lessons_text.split())
        
        prompt = f"""Compress these {lesson_type} lessons by merging similar rules.

                    Current lessons ({current_words} words):
                    {lessons_text}

                    Target: Reduce to approximately {target_words} words (50% compression).

                    Compression guidelines:
                    - Merge 2-3 similar rules into one generalized rule
                    - Remove redundant or overlapping rules
                    - Keep the numbered format (1., 2., 3., etc.)
                    - Preserve specific, actionable guidance
                    - Each rule should be max 30 words
                    - Prioritize rules that prevent common errors

                    Example compression:
                    Before:
                    1. When user mentions "products", include products table
                    2. When user mentions "categories", include categories table
                    3. When user mentions "customers", include customers table

                    After:
                    1. When user mentions an entity by name, include the corresponding table that stores that entity's details

                    Output ONLY the compressed rules, no explanations or commentary.
            """
        
        try:
            result = self.openai_client.generate_structured_completion(
                messages=[
                    {
                        "role": "system",
                        "content": "You are compressing database query rules. Merge similar rules into general principles. Be concise but preserve meaning."
                    },
                    {"role": "user", "content": prompt}
                ],
                response_model=CompressionOutput,
                model="gpt-4o-mini",
                temperature=0.0
            )
            
            compressed = result.compressed_lessons
            logger.info(f"Compressed {lesson_type} lessons: {current_words} -> {len(compressed.split())} words")
            
            return compressed
            
        except Exception as e:
            logger.error(f"LLM compression failed: {e}")
            # Return original if compression fails
            return lessons_text
        
    def _save_compressed_summary(
        self,
        kg_id: UUID,
        schema_lessons: str,
        sql_lessons: str,
        lesson_count: int,
        word_count: int
    ) -> bool:
        
        query = """
            UPDATE kg_error_summary
            SET schema_lessons = %s,
                sql_lessons = %s,
                lesson_count = %s,
                word_count = %s,
                last_compressed_at = CURRENT_TIMESTAMP,
                last_updated = CURRENT_TIMESTAMP,
                version = version + 1
            WHERE kg_id = %s
        """ 
        
        try:
            self.conn.rollback()
            
            with self.conn.cursor() as cur:
                cur.execute(query, (
                    schema_lessons,
                    sql_lessons,
                    lesson_count,
                    word_count,
                    str(kg_id)
                ))
                
                self.conn.commit()
                logger.info(f"Saved compressed summary: {lesson_count} lessons, {word_count} words")
                return True
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to save compressed summary: {e}")
            return False
