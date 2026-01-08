import logging
from typing import List, Set, Dict
from collections import defaultdict, deque

from ...kg.models import KnowledgeGraph

logger = logging.getLogger(__name__)

class GraphTraversalTool:
    """Traverses KG relationship graph to find connection paths between tables"""
    
    def find_bridging_tables(
        self,
        kg: KnowledgeGraph,
        selected_tables: List[str]
    ) -> List[str]:
        """
            Find bridging tables needed to connect selected tables.
        """
        
        if len(selected_tables) <= 1:
            logger.info("Only one table selected, no bridging needed")
            return []
        
        logger.info(f"Finding bridging tables for: {selected_tables}")
        
        # Build adjacency list from relationships
        graph = self._build_graph(kg)
        
        # Find all pairs of selected tables
        bridging_tables = set()
        
        for i in range(len(selected_tables)):
            for j in range(i + 1, len(selected_tables)):
                table_a = selected_tables[i]
                table_b = selected_tables[j]
                
                # Find path between table_a and table_b
                path = self._find_shortest_path(graph, table_a, table_b)
                
                if path and len(path) > 2:
                    # Path has intermediate tables
                    intermediate = path[1:-1]  # Exclude start and end
                    bridging_tables.update(intermediate)
                    logger.info(f"Path {table_a} -> {table_b}: {' -> '.join(path)}")
                elif path:
                    logger.info(f"Direct connection: {table_a} -> {table_b}")
                else:
                    logger.warning(f"No path found between {table_a} and {table_b}")
        
        # Remove any tables that are already in selected_tables
        bridging_tables = bridging_tables - set(selected_tables)
        
        bridging_list = list(bridging_tables)
        
        if bridging_list:
            logger.info(f"Bridging tables found: {bridging_list}")
        else:
            logger.info("No bridging tables needed")
        
        return bridging_list
    
    def _build_graph(self, kg: KnowledgeGraph) -> Dict[str, Set[str]]:
        """
            Build undirected adjacency list from relationships
        """
        graph = defaultdict(set)
        
        for rel in kg.relationships:
            # Bidirectional edges (undirected graph)
            graph[rel.from_table_name].add(rel.to_table_name)
            graph[rel.to_table_name].add(rel.from_table_name)
        
        return graph
    
    def _find_shortest_path(
        self,
        graph: Dict[str, Set[str]],
        start: str,
        end: str
    ) -> List[str]:
        """
            Find shortest path between two tables using BFS.
        """
        if start == end:
            return [start]
        
        if start not in graph:
            return None
        
        # BFS
        queue = deque([(start, [start])])
        visited = {start}
        
        while queue:
            current, path = queue.popleft()
            
            for neighbor in graph[current]:
                if neighbor == end:
                    return path + [neighbor]
                
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [neighbor]))
        
        return None
    
    def validate_connections(
        self,
        kg: KnowledgeGraph,
        all_tables: List[str]
    ) -> bool:
        """
            Validate that all tables are connected through relationships.
        """
        if len(all_tables) <= 1:
            return True
        
        graph = self._build_graph(kg)
        
        # Check if all tables are in one connected component
        start_table = all_tables[0]
        reachable = self._get_connected_component(graph, start_table)
        
        for table in all_tables:
            if table not in reachable:
                logger.warning(f"Table '{table}' is not connected to others")
                return False
        
        return True
    
    def _get_connected_component(
        self,
        graph: Dict[str, Set[str]],
        start: str
    ) -> Set[str]:
        """
            Get all tables in the same connected component as start
        """
        visited = set()
        queue = deque([start])
        
        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            
            visited.add(current)
            
            for neighbor in graph.get(current, []):
                if neighbor not in visited:
                    queue.append(neighbor)
        
        return visited
    