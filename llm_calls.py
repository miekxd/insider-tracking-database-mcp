import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, date, timedelta
from mcp.server.fastmcp import FastMCP
from db import execute_query, execute_write

logger = logging.getLogger(__name__)

def register_llm_calls_tools(mcp: FastMCP):
    """
    Register all LLM calls tools with the MCP server.
    """
    
    @mcp.tool()
    def get_llm_calls(
        ticker: Optional[str] = None,
        status: Optional[str] = None,
        recommendation: Optional[str] = None,
        is_closed: Optional[bool] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Query LLM calls with various filters.
        
        Args:
            ticker: Filter by stock ticker symbol
            status: Filter by call status (e.g., 'OPEN', 'CLOSED')
            recommendation: Filter by recommendation type (e.g., 'BUY', 'SELL', 'HOLD')
            is_closed: Filter by closed status
            start_date: Start date for entry_date filter (YYYY-MM-DD)
            end_date: End date for entry_date filter (YYYY-MM-DD)
            limit: Maximum number of results (default: 100, max: 1000)
            offset: Offset for pagination (default: 0)
            
        Returns:
            Dict with LLM call results and metadata
        """
        try:
            # Validate limit
            if limit < 1 or limit > 1000:
                limit = 100
            
            # Build query
            conditions = []
            params = []
            
            if ticker:
                conditions.append("ticker = %s")
                params.append(ticker.upper())
            
            if status:
                conditions.append("status = %s")
                params.append(status.upper())
            
            if recommendation:
                conditions.append("recommendation = %s")
                params.append(recommendation.upper())
            
            if is_closed is not None:
                conditions.append("is_closed = %s")
                params.append(is_closed)
            
            if start_date:
                conditions.append("entry_date >= %s")
                params.append(start_date)
            
            if end_date:
                conditions.append("entry_date <= %s")
                params.append(end_date)
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            query = f"""
                SELECT * FROM llm_calls
                WHERE {where_clause}
                ORDER BY entry_date DESC, call_date DESC
                LIMIT %s OFFSET %s
            """
            
            params.append(limit)
            params.append(offset)
            
            # Get count for pagination
            count_query = f"""
                SELECT COUNT(*) as total FROM llm_calls
                WHERE {where_clause}
            """
            
            results = execute_query(query, tuple(params))
            count_result = execute_query(count_query, tuple(params[:-2]), fetch_one=True)
            total = count_result['total'] if count_result else 0
            
            logger.info(f"Retrieved {len(results)} LLM calls")
            
            return {
                "calls": results,
                "count": len(results),
                "total": total,
                "limit": limit,
                "offset": offset,
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_llm_calls: {str(e)}")
            raise Exception(f"Failed to retrieve LLM calls: {str(e)}")
    
    @mcp.tool()
    def get_call_by_id(call_id: int) -> Dict[str, Any]:
        """
        Get a specific LLM call by ID.
        
        Args:
            call_id: The call ID
            
        Returns:
            Dict with call details
        """
        try:
            query = "SELECT * FROM llm_calls WHERE id = %s"
            result = execute_query(query, (call_id,), fetch_one=True)
            
            if not result:
                raise Exception(f"Call not found: {call_id}")
            
            return {
                "call": result,
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_call_by_id: {str(e)}")
            raise Exception(f"Failed to retrieve call: {str(e)}")
    
    @mcp.tool()
    def get_open_calls(limit: int = 100) -> Dict[str, Any]:
        """
        Get all open/active calls that haven't been closed yet.
        
        Args:
            limit: Maximum number of results (default: 100, max: 1000)
            
        Returns:
            Dict with open calls
        """
        try:
            if limit < 1 or limit > 1000:
                limit = 100
            
            query = """
                SELECT * FROM llm_calls
                WHERE is_closed = FALSE
                ORDER BY entry_date DESC, call_date DESC
                LIMIT %s
            """
            
            results = execute_query(query, (limit,))
            
            logger.info(f"Retrieved {len(results)} open calls")
            
            return {
                "calls": results,
                "count": len(results),
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_open_calls: {str(e)}")
            raise Exception(f"Failed to retrieve open calls: {str(e)}")
    
    @mcp.tool()
    def get_call_performance(
        days: int = 30,
        ticker: Optional[str] = None,
        recommendation: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get performance metrics for LLM calls.
        
        Args:
            days: Number of days to analyze (default: 30)
            ticker: Filter by stock ticker symbol (optional)
            recommendation: Filter by recommendation type (optional)
            
        Returns:
            Dict with performance statistics
        """
        try:
            conditions = []
            params = []
            
            start_date = (datetime.now() - timedelta(days=days)).date()
            conditions.append("entry_date >= %s")
            params.append(start_date)
            
            if ticker:
                conditions.append("ticker = %s")
                params.append(ticker.upper())
            
            if recommendation:
                conditions.append("recommendation = %s")
                params.append(recommendation.upper())
            
            where_clause = " AND ".join(conditions)
            
            query = f"""
                SELECT 
                    COUNT(*) as total_calls,
                    COUNT(CASE WHEN is_closed = TRUE THEN 1 END) as closed_calls,
                    COUNT(CASE WHEN is_closed = FALSE THEN 1 END) as open_calls,
                    AVG(price_change_pct) as avg_price_change_pct,
                    SUM(pnl_dollars) as total_pnl,
                    AVG(pnl_dollars) as avg_pnl,
                    AVG(holding_days) as avg_holding_days,
                    COUNT(CASE WHEN pnl_dollars > 0 THEN 1 END) as winning_calls,
                    COUNT(CASE WHEN pnl_dollars < 0 THEN 1 END) as losing_calls,
                    COUNT(CASE WHEN pnl_dollars IS NULL THEN 1 END) as pending_calls
                FROM llm_calls
                WHERE {where_clause}
            """
            
            result = execute_query(query, tuple(params), fetch_one=True)
            
            # Calculate win rate
            closed_count = result.get('closed_calls', 0) or 0
            winning_count = result.get('winning_calls', 0) or 0
            win_rate = (winning_count / closed_count * 100) if closed_count > 0 else 0
            
            result['win_rate_pct'] = round(win_rate, 2)
            
            return {
                "performance": result,
                "days": days,
                "filters": {
                    "ticker": ticker,
                    "recommendation": recommendation
                },
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_call_performance: {str(e)}")
            raise Exception(f"Failed to retrieve call performance: {str(e)}")
    
    @mcp.tool()
    def get_calls_by_batch(batch_id: str) -> Dict[str, Any]:
        """
        Get all LLM calls from a specific batch.
        
        Args:
            batch_id: The batch ID to filter by
            
        Returns:
            Dict with calls from the specified batch
        """
        try:
            query = """
                SELECT * FROM llm_calls
                WHERE batch_id = %s
                ORDER BY rank ASC, call_date DESC
            """
            
            results = execute_query(query, (batch_id,))
            
            logger.info(f"Retrieved {len(results)} calls for batch {batch_id}")
            
            return {
                "calls": results,
                "count": len(results),
                "batch_id": batch_id,
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_calls_by_batch: {str(e)}")
            raise Exception(f"Failed to retrieve calls by batch: {str(e)}")

