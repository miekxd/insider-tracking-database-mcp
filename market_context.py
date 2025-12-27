import logging
import json
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from mcp.server.fastmcp import FastMCP
from db import execute_query, execute_write

logger = logging.getLogger(__name__)

def register_market_context_tools(mcp: FastMCP):
    """
    Register all market context tools with the MCP server.
    """
    
    @mcp.tool()
    def get_market_context(
        start_timestamp: Optional[str] = None,
        end_timestamp: Optional[str] = None,
        batch_id: Optional[str] = None,
        limit: int = 50
    ) -> Dict[str, Any]:
        """
        Get market context records with various filters.
        
        Args:
            start_timestamp: Start timestamp filter (ISO format)
            end_timestamp: End timestamp filter (ISO format)
            batch_id: Filter by batch ID (optional, searches in JSONB fields)
            limit: Maximum number of results (default: 50, max: 500)
            
        Returns:
            Dict with market context results and metadata
        """
        try:
            # Validate limit
            if limit < 1 or limit > 500:
                limit = 50
            
            # Build query
            conditions = []
            params = []
            
            if start_timestamp:
                conditions.append("timestamp >= %s")
                params.append(start_timestamp)
            
            if end_timestamp:
                conditions.append("timestamp <= %s")
                params.append(end_timestamp)
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            query = f"""
                SELECT * FROM market_context
                WHERE {where_clause}
                ORDER BY timestamp DESC
                LIMIT %s
            """
            
            params.append(limit)
            
            results = execute_query(query, tuple(params))
            
            # Filter by batch_id if provided (search in JSONB fields)
            if batch_id and results:
                filtered_results = []
                for result in results:
                    # Check if batch_id appears in any JSONB field
                    found = False
                    for field in ['sector_activity', 'ceo_cfo_buys', 'large_transactions', 'notable_patterns']:
                        if result.get(field) and batch_id in json.dumps(result[field]):
                            found = True
                            break
                    if found:
                        filtered_results.append(result)
                results = filtered_results
            
            logger.info(f"Retrieved {len(results)} market context records")
            
            return {
                "market_contexts": results,
                "count": len(results),
                "limit": limit,
                "filters": {
                    "start_timestamp": start_timestamp,
                    "end_timestamp": end_timestamp,
                    "batch_id": batch_id
                },
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_market_context: {str(e)}")
            raise Exception(f"Failed to retrieve market context: {str(e)}")
    
    @mcp.tool()
    def get_latest_market_context() -> Dict[str, Any]:
        """
        Get the most recent market context record.
        
        Returns:
            Dict with the latest market context
        """
        try:
            query = """
                SELECT * FROM market_context
                ORDER BY timestamp DESC
                LIMIT 1
            """
            
            result = execute_query(query, None, fetch_one=True)
            
            if not result:
                return {
                    "market_context": None,
                    "message": "No market context records found",
                    "timestamp": datetime.now().isoformat(),
                    "status": "success"
                }
            
            logger.info("Retrieved latest market context")
            
            return {
                "market_context": result,
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_latest_market_context: {str(e)}")
            raise Exception(f"Failed to retrieve latest market context: {str(e)}")
    
    @mcp.tool()
    def get_market_context_by_id(context_id: int) -> Dict[str, Any]:
        """
        Get a specific market context record by ID.
        
        Args:
            context_id: The market context ID
            
        Returns:
            Dict with market context details
        """
        try:
            query = "SELECT * FROM market_context WHERE id = %s"
            result = execute_query(query, (context_id,), fetch_one=True)
            
            if not result:
                raise Exception(f"Market context not found: {context_id}")
            
            return {
                "market_context": result,
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_market_context_by_id: {str(e)}")
            raise Exception(f"Failed to retrieve market context: {str(e)}")
    
    @mcp.tool()
    def get_market_context_summary(days: int = 7) -> Dict[str, Any]:
        """
        Get summary statistics of market context over a time period.
        
        Args:
            days: Number of days to analyze (default: 7)
            
        Returns:
            Dict with summary statistics
        """
        try:
            start_timestamp = (datetime.now() - timedelta(days=days)).isoformat()
            
            query = """
                SELECT 
                    COUNT(*) as total_records,
                    MIN(timestamp) as earliest_timestamp,
                    MAX(timestamp) as latest_timestamp,
                    AVG(batch_size) as avg_batch_size,
                    SUM(batch_size) as total_batch_size
                FROM market_context
                WHERE timestamp >= %s
            """
            
            result = execute_query(query, (start_timestamp,), fetch_one=True)
            
            return {
                "summary": result,
                "days": days,
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_market_context_summary: {str(e)}")
            raise Exception(f"Failed to retrieve market context summary: {str(e)}")

