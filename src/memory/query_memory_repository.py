import logging
import json
import numpy as np
from typing import List, Dict, Any, Optional
from uuid import UUID, uuid4
from psycopg2.extras import RealDictCursor, execute_values


logger = logging.getLogger(__name__)


class QueryMemoryRepository:
    """Manages query logs and error patterns in PostgreSQL"""
    
    def __init__(self, kg_conn):
        self.conn = kg_conn
        
    def insert_query_log(self, query_data: Dict[str, Any]) -> bool:
        """
            Insert query log into kg_query_log table.
        """
        logger.info("Inserting query log")
        
        # IMPORTANT: Rollback any previous failed transactions
        try:
            self.conn.rollback()
        except:
            pass
        
        query = """
            INSERT INTO kg_query_log (
                query_id, kg_id, user_question, refined_query, intent_summary,
                selected_tables, generated_sql, execution_success, execution_time_ms,
                error_message, error_category, correction_summary, tables_used,
                correction_applied, iterations_count, schema_retrieval_time_ms,
                sql_generation_time_ms, confidence_score, query_embedding, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s::vector, NOW()
            )
            RETURNING query_id
        """
        
        try:
            embedding_json = None
            if "query_embedding" in query_data and query_data["query_embedding"]:
                embedding_list = query_data["query_embedding"]
                embedding_str = '[' + ','.join(map(str, embedding_list)) + ']'
                logger.debug(f"Embedding length: {len(embedding_list)} dimensions")
            
            with self.conn.cursor() as cur:
                cur.execute(query, (
                    str(uuid4()),
                    query_data["kg_id"],
                    query_data["user_question"],
                    query_data.get("refined_query"),
                    query_data.get("intent_summary"),
                    json.dumps(query_data.get("selected_tables", [])),
                    query_data["generated_sql"],
                    query_data["execution_success"],
                    query_data.get("execution_time_ms"),
                    query_data.get("error_message"),
                    query_data.get("error_category"),
                    query_data.get("correction_summary"),
                    json.dumps(query_data.get("tables_used", [])),
                    query_data.get("correction_applied", False),
                    query_data.get("iterations_count", 1),
                    query_data.get("schema_retrieval_time_ms"),
                    query_data.get("sql_generation_time_ms"),
                    query_data.get("confidence_score"),
                    embedding_str
                ))
                
                result = cur.fetchone()
                self.conn.commit()
                
                if result:
                    returned_id = str(result[0])
                    logger.info(f"Query log inserted successfully with id: {returned_id}")
                    return returned_id
                return None
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to insert query log: {e}")
            return None
        
    def update_query_feedback(
        self,
        query_log_id: UUID,
        feedback: str,
        rating: Optional[int] = None
    ) -> bool:
        """
            Update user feedback for a specific query log entry.
        """
        logger.info(f"Updating feedback for query: {query_log_id}")
        
        try:
            self.conn.rollback()
        except:
            pass
        
        query = """
            UPDATE kg_query_log
            SET user_feedback = %s
            WHERE query_id = %s
            RETURNING query_id
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (feedback, str(query_log_id)))
                result = cur.fetchone()
                self.conn.commit()
                
                if result:
                    logger.info(f"Feedback updated successfully for query {query_log_id}")
                    return True
                else:
                    logger.warning(f"No query found with id {query_log_id}")
                    return False
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to update query feedback: {e}")
            return False
        
    def get_query_log_by_id(self, query_log_id: UUID) -> Optional[Dict[str, Any]]:
        """
            Retrieve complete query log entry by query_id.
            Used for feedback-based lesson extraction.
        """
        logger.info(f"Retrieving query log: {query_log_id}")
        
        try:
            self.conn.rollback()
        except:
            pass
        
        query = """
            SELECT 
                query_id,
                kg_id,
                user_question,
                refined_query,
                intent_summary,
                selected_tables,
                generated_sql,
                execution_success,
                execution_time_ms,
                error_message,
                error_category,
                correction_summary,
                tables_used,
                correction_applied,
                iterations_count,
                schema_retrieval_time_ms,
                sql_generation_time_ms,
                confidence_score,
                user_feedback,
                created_at
            FROM kg_query_log
            WHERE query_id = %s
        """
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (str(query_log_id),))
                result = cur.fetchone()
                
                if result:
                    query_log = dict(result)
                    
                    # Parse JSON fields
                    if query_log.get('selected_tables'):
                        if isinstance(query_log['selected_tables'], str):
                            query_log['selected_tables'] = json.loads(query_log['selected_tables'])
                            
                    if query_log.get('tables_used'):
                        if isinstance(query_log['tables_used'], str):
                            query_log['tables_used'] = json.loads(query_log['tables_used'])
                            
                    logger.info(f"Retrieved query log successfully")
                    return query_log
                
                else:
                    logger.warning(f"No query log found with id: {query_log_id}")
                    return None
                
        except Exception as e:
            logger.error(f"Failed to retrieve query log: {e}")
            return None
    
    def get_error_patterns_for_query(
        self,
        kg_id: str,
        error_category: Optional[str] = None,
        affected_tables: Optional[List[str]] = None,
        limit: int = 3
    ) -> List[Dict[str, Any]]:
        
        """
            Retrieve error patterns relevant to a specific query.
        """
        
        logger.info(f"Retrieving error patterns for kg_id={kg_id}, category={error_category}")
    
        # Rollback any previous failed transactions
        try:
            self.conn.rollback()
        except:
            pass
        
        # Build query with flexible matching
        query = """
            SELECT 
                pattern_id,
                error_category,
                error_pattern,
                example_error_message,
                fix_applied,
                affected_tables,
                occurrence_count,
                success_rate_after_fix,
                last_seen
            FROM query_error_patterns
            WHERE kg_id = %s
                AND is_active = true
        """
        params = [kg_id]
        
        # Add error category filter if provided
        if error_category:
            query += " AND error_category = %s"
            params.append(error_category)
            
        # Add table overlap filter if provided
        if affected_tables and len(affected_tables) > 0:
            # Find patterns where affected_tables overlap
            query += " AND affected_tables ?| %s"  # PostgreSQL JSONB overlap operator
            params.append(affected_tables)
        
        # Order by relevance: occurrence count and recency
        query += " ORDER BY occurrence_count DESC, last_seen DESC LIMIT %s"
        params.append(limit)
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                results = cur.fetchall()
                
                patterns = []
                for row in results:
                    pattern_dict = dict(row)
                    
                    # Parse JSON field
                    if pattern_dict.get('affected_tables'):
                        if isinstance(pattern_dict['affected_tables'], str):
                            pattern_dict['affected_tables'] = json.loads(pattern_dict['affected_tables'])
                    
                    patterns.append(pattern_dict)
                
                logger.info(f"Retrieved {len(patterns)} relevant error patterns")
                return patterns
                
        except Exception as e:
            logger.error(f"Failed to retrieve error patterns: {e}")
            return []
        
        
    def search_similar_queries(
        self,
        kg_id: str,
        query_embedding: List[float],
        limit: int = 5,
        only_successful: bool = True
    ) -> List[Dict[str, Any]]:
        """
            Search for similar queries using vector similarity.
        """
        logger.info(f"Searching for similar queries (limit={limit})")
        
        # Convert embedding list to PostgreSQL vector format
        # Format: '[val1,val2,val3,...]'
        embedding_str = '[' + ','.join(map(str, query_embedding)) + ']'
        
        # Build query with pgvector's cosine distance operator (<=>)
        # Note: <=> returns distance (0 = identical, 2 = opposite)
        # We convert to similarity: 1 - (distance / 2) to get 0-1 scale
        query = """
            SELECT 
                query_id,
                user_question,
                generated_sql,
                execution_success,
                tables_used,
                confidence_score,
                created_at,
                1 - (query_embedding <=> %s::vector) / 2 AS similarity
            FROM kg_query_log
            WHERE kg_id = %s
                AND execution_success = %s
                AND query_embedding IS NOT NULL
            ORDER BY query_embedding <=> %s::vector
            LIMIT %s
        """
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (embedding_str, kg_id, only_successful, embedding_str, limit))
                results = cur.fetchall()
                
                if not results:
                    logger.info("No similar queries found with embeddings")
                    return []
                
                formatted_results = []
                for row in results:
                    formatted_results.append({
                        "query_id": str(row["query_id"]),
                        "user_question": row["user_question"],
                        "generated_sql": row["generated_sql"],
                        "execution_success": row["execution_success"],
                        "tables_used": row["tables_used"] if row["tables_used"] else [],
                        "confidence_score": float(row["confidence_score"]) if row["confidence_score"] else 0.0,
                        "similarity": float(row["similarity"]) if row.get("similarity") else 0.0
                    })
                
                logger.info(f"Found {len(formatted_results)} similar queries")
                
                for i, query_result in enumerate(formatted_results[:3], 1):
                    logger.info(
                        f"  {i}. '{query_result['user_question'][:60]}...' "
                        f"(similarity: {query_result['similarity']:.3f})"
                    )
                
                return formatted_results
                
        except Exception as e:
            logger.error(f"Failed to search similar queries: {e}")
            return []
    
    def get_error_patterns(
        self,
        kg_id: str,
        error_category: Optional[str] = None,
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """
            Retrieve error patterns from query_error_patterns table.
        """
        logger.info(f"Retrieving error patterns (category={error_category})")
        
        # IMPORTANT: Rollback any previous failed transactions
        try:
            self.conn.rollback()
        except:
            pass
        
        query = """
            SELECT 
                pattern_id,
                error_category,
                error_pattern,
                example_error_message,
                fix_applied,
                occurrence_count,
                success_rate_after_fix,
                last_seen
            FROM query_error_patterns
            WHERE kg_id = %s
                AND is_active = true
        """
        
        params = [kg_id]
        
        if error_category:
            query += " AND error_category = %s"
            params.append(error_category)
        
        query += " ORDER BY occurrence_count DESC LIMIT %s"
        params.append(limit)
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                results = cur.fetchall()
                
                patterns = []
                for row in results:
                    patterns.append({
                        "pattern_id": str(row["pattern_id"]),
                        "error_category": row["error_category"],
                        "error_pattern": row["error_pattern"],
                        "example_error_message": row["example_error_message"],
                        "fix_applied": row["fix_applied"],
                        "occurrence_count": row["occurrence_count"],
                        "success_rate_after_fix": float(row["success_rate_after_fix"]) if row["success_rate_after_fix"] else None
                    })
                
                logger.info(f"Retrieved {len(patterns)} error patterns")
                return patterns
                
        except Exception as e:
            logger.error(f"Failed to retrieve error patterns: {e}")
            return []
    
    def insert_error_pattern(self, pattern_data: Dict[str, Any]) -> bool:
        """
            Insert or update error pattern.
        """
        logger.info("Inserting/updating error pattern")
        
        # IMPORTANT: Rollback any previous failed transactions
        try:
            self.conn.rollback()
        except:
            pass
        
        query = """
            INSERT INTO query_error_patterns (
                pattern_id, kg_id, error_category, error_pattern,
                example_error_message, fix_applied, affected_tables,
                occurrence_count, first_seen, last_seen
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            ON CONFLICT (kg_id, error_pattern)
            DO UPDATE SET
                occurrence_count = query_error_patterns.occurrence_count + 1,
                last_seen = CURRENT_TIMESTAMP,
                example_error_message = EXCLUDED.example_error_message
            RETURNING pattern_id
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (
                    str(uuid4()),
                    pattern_data["kg_id"],
                    pattern_data["error_category"],
                    pattern_data["error_pattern"],
                    pattern_data.get("example_error_message"),
                    pattern_data["fix_applied"],
                    json.dumps(pattern_data.get("affected_tables", [])),
                    1  # Initial occurrence count
                ))
                
                self.conn.commit()
                logger.info("Error pattern inserted/updated successfully")
                return True
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to insert error pattern: {e}")
            return False
            