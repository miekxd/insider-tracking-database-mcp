import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, date, timedelta
from mcp.server.fastmcp import FastMCP
from db import execute_query, execute_write

logger = logging.getLogger(__name__)

def register_insider_transaction_tools(mcp: FastMCP):
    """
    Register all insider transaction tools with the MCP server.
    """
    
    @mcp.tool()
    def get_insider_transactions(
        ticker: Optional[str] = None,
        insider_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        signal_generated: Optional[bool] = None,
        signal_quality: Optional[str] = None,
        alert_sent: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Query insider transactions with various filters.
        
        Args:
            ticker: Filter by stock ticker symbol
            insider_name: Filter by insider name
            start_date: Start date for transaction_date filter (YYYY-MM-DD)
            end_date: End date for transaction_date filter (YYYY-MM-DD)
            signal_generated: Filter by signal generation status
            signal_quality: Filter by signal quality (e.g., 'high', 'medium', 'low')
            alert_sent: Filter by alert sent status
            limit: Maximum number of results (default: 100, max: 1000)
            offset: Offset for pagination (default: 0)
            
        Returns:
            Dict with transaction results and metadata
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
            
            if insider_name:
                conditions.append("insider_name ILIKE %s")
                params.append(f"%{insider_name}%")
            
            if start_date:
                conditions.append("transaction_date >= %s")
                params.append(start_date)
            
            if end_date:
                conditions.append("transaction_date <= %s")
                params.append(end_date)
            
            if signal_generated is not None:
                conditions.append("signal_generated = %s")
                params.append(signal_generated)
            
            if signal_quality:
                conditions.append("signal_quality = %s")
                params.append(signal_quality)
            
            if alert_sent is not None:
                conditions.append("alert_sent = %s")
                params.append(alert_sent)
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            query = f"""
                SELECT * FROM insider_transactions
                WHERE {where_clause}
                ORDER BY transaction_date DESC NULLS LAST, filing_date DESC
                LIMIT %s OFFSET %s
            """
            
            params.append(limit)
            params.append(offset)
            
            # Get count for pagination
            count_query = f"""
                SELECT COUNT(*) as total FROM insider_transactions
                WHERE {where_clause}
            """
            
            results = execute_query(query, tuple(params))
            count_result = execute_query(count_query, tuple(params[:-2]), fetch_one=True)
            total = count_result['total'] if count_result else 0
            
            logger.info(f"Retrieved {len(results)} insider transactions")
            
            return {
                "transactions": results,
                "count": len(results),
                "total": total,
                "limit": limit,
                "offset": offset,
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_insider_transactions: {str(e)}")
            raise Exception(f"Failed to retrieve insider transactions: {str(e)}")
    
    @mcp.tool()
    def get_transaction_by_id(transaction_id: str) -> Dict[str, Any]:
        """
        Get a specific insider transaction by ID.
        
        Args:
            transaction_id: The transaction ID (can be numeric ID or transaction_id field)
            
        Returns:
            Dict with transaction details
        """
        try:
            # Try numeric ID first
            if transaction_id.isdigit():
                query = "SELECT * FROM insider_transactions WHERE id = %s"
                result = execute_query(query, (int(transaction_id),), fetch_one=True)
                if result:
                    return {
                        "transaction": result,
                        "timestamp": datetime.now().isoformat(),
                        "status": "success"
                    }
            
            # Try transaction_id field
            query = "SELECT * FROM insider_transactions WHERE transaction_id = %s"
            result = execute_query(query, (transaction_id,), fetch_one=True)
            
            if not result:
                raise Exception(f"Transaction not found: {transaction_id}")
            
            return {
                "transaction": result,
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_transaction_by_id: {str(e)}")
            raise Exception(f"Failed to retrieve transaction: {str(e)}")
    
    @mcp.tool()
    def get_recent_signals(
        days: int = 7,
        signal_quality: Optional[str] = None,
        limit: int = 50
    ) -> Dict[str, Any]:
        """
        Get recent transactions with signals generated.
        
        Args:
            days: Number of days to look back (default: 7)
            signal_quality: Filter by signal quality (optional)
            limit: Maximum number of results (default: 50, max: 500)
            
        Returns:
            Dict with recent signal transactions
        """
        try:
            if limit < 1 or limit > 500:
                limit = 50
            
            conditions = ["signal_generated = TRUE"]
            params = []
            
            start_date = (datetime.now() - timedelta(days=days)).date()
            conditions.append("filing_date >= %s")
            params.append(start_date)
            
            if signal_quality:
                conditions.append("signal_quality = %s")
                params.append(signal_quality)
            
            where_clause = " AND ".join(conditions)
            
            params.append(limit)
            
            query = f"""
                SELECT * FROM insider_transactions
                WHERE {where_clause}
                ORDER BY filing_date DESC, signal_score DESC NULLS LAST
                LIMIT %s
            """
            
            results = execute_query(query, tuple(params))
            
            logger.info(f"Retrieved {len(results)} recent signals")
            
            return {
                "signals": results,
                "count": len(results),
                "days": days,
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_recent_signals: {str(e)}")
            raise Exception(f"Failed to retrieve recent signals: {str(e)}")
    
    @mcp.tool()
    def get_unprocessed_transactions(limit: int = 100) -> Dict[str, Any]:
        """
        Get transactions that haven't been processed for signal generation yet.
        
        Args:
            limit: Maximum number of results (default: 100, max: 1000)
            
        Returns:
            Dict with unprocessed transactions
        """
        try:
            if limit < 1 or limit > 1000:
                limit = 100
            
            query = """
                SELECT * FROM insider_transactions
                WHERE signal_generated = FALSE
                ORDER BY filing_date DESC
                LIMIT %s
            """
            
            results = execute_query(query, (limit,))
            
            logger.info(f"Retrieved {len(results)} unprocessed transactions")
            
            return {
                "transactions": results,
                "count": len(results),
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_unprocessed_transactions: {str(e)}")
            raise Exception(f"Failed to retrieve unprocessed transactions: {str(e)}")
    
    @mcp.tool()
    def get_insider_stats(
        ticker: Optional[str] = None,
        insider_name: Optional[str] = None,
        days: int = 30
    ) -> Dict[str, Any]:
        """
        Get statistics about insider transactions.
        
        Args:
            ticker: Filter by stock ticker symbol (optional)
            insider_name: Filter by insider name (optional)
            days: Number of days to analyze (default: 30)
            
        Returns:
            Dict with statistics and aggregations
        """
        try:
            conditions = []
            params = []
            
            start_date = (datetime.now() - timedelta(days=days)).date()
            conditions.append("transaction_date >= %s")
            params.append(start_date)
            
            if ticker:
                conditions.append("ticker = %s")
                params.append(ticker.upper())
            
            if insider_name:
                conditions.append("insider_name ILIKE %s")
                params.append(f"%{insider_name}%")
            
            where_clause = " AND ".join(conditions)
            
            query = f"""
                SELECT 
                    COUNT(*) as total_transactions,
                    COUNT(DISTINCT ticker) as unique_tickers,
                    COUNT(DISTINCT insider_name) as unique_insiders,
                    SUM(transaction_value) as total_value,
                    AVG(transaction_value) as avg_value,
                    SUM(shares) as total_shares,
                    COUNT(CASE WHEN signal_generated = TRUE THEN 1 END) as signals_generated,
                    COUNT(CASE WHEN alert_sent = TRUE THEN 1 END) as alerts_sent
                FROM insider_transactions
                WHERE {where_clause}
            """
            
            result = execute_query(query, tuple(params), fetch_one=True)
            
            return {
                "statistics": result,
                "days": days,
                "filters": {
                    "ticker": ticker,
                    "insider_name": insider_name
                },
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_insider_stats: {str(e)}")
            raise Exception(f"Failed to retrieve insider statistics: {str(e)}")

