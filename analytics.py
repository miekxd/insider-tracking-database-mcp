import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from mcp.server.fastmcp import FastMCP
from db import execute_query, execute_write

logger = logging.getLogger(__name__)

def register_analytics_tools(mcp: FastMCP):
    """
    Register all analytics tools with the MCP server.
    """
    
    @mcp.tool()
    def get_portfolio_summary(days: int = 30) -> Dict[str, Any]:
        """
        Get overall portfolio performance summary.
        
        Args:
            days: Number of days to analyze (default: 30)
            
        Returns:
            Dict with portfolio summary statistics
        """
        try:
            start_date = (datetime.now() - timedelta(days=days)).date()
            
            # Get LLM calls summary
            calls_query = """
                SELECT 
                    COUNT(*) as total_calls,
                    COUNT(CASE WHEN is_closed = TRUE THEN 1 END) as closed_calls,
                    COUNT(CASE WHEN is_closed = FALSE THEN 1 END) as open_calls,
                    SUM(pnl_dollars) as total_pnl,
                    AVG(pnl_dollars) as avg_pnl,
                    AVG(price_change_pct) as avg_price_change_pct,
                    COUNT(CASE WHEN pnl_dollars > 0 THEN 1 END) as winning_calls,
                    COUNT(CASE WHEN pnl_dollars < 0 THEN 1 END) as losing_calls
                FROM llm_calls
                WHERE entry_date >= %s
            """
            
            calls_result = execute_query(calls_query, (start_date,), fetch_one=True)
            
            # Get insider transactions summary
            transactions_query = """
                SELECT 
                    COUNT(*) as total_transactions,
                    COUNT(DISTINCT ticker) as unique_tickers,
                    COUNT(DISTINCT insider_name) as unique_insiders,
                    SUM(transaction_value) as total_transaction_value,
                    COUNT(CASE WHEN signal_generated = TRUE THEN 1 END) as signals_generated,
                    COUNT(CASE WHEN alert_sent = TRUE THEN 1 END) as alerts_sent
                FROM insider_transactions
                WHERE transaction_date >= %s
            """
            
            transactions_result = execute_query(transactions_query, (start_date,), fetch_one=True)
            
            # Calculate win rate
            closed_count = calls_result.get('closed_calls', 0) or 0
            winning_count = calls_result.get('winning_calls', 0) or 0
            win_rate = (winning_count / closed_count * 100) if closed_count > 0 else 0
            
            return {
                "portfolio_summary": {
                    "llm_calls": calls_result,
                    "insider_transactions": transactions_result,
                    "win_rate_pct": round(win_rate, 2)
                },
                "days": days,
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_portfolio_summary: {str(e)}")
            raise Exception(f"Failed to retrieve portfolio summary: {str(e)}")
    
    @mcp.tool()
    def get_ticker_analysis(ticker: str, days: int = 30) -> Dict[str, Any]:
        """
        Get comprehensive analysis for a specific ticker.
        
        Args:
            ticker: Stock ticker symbol
            days: Number of days to analyze (default: 30)
            
        Returns:
            Dict with ticker analysis including transactions and calls
        """
        try:
            ticker = ticker.upper()
            start_date = (datetime.now() - timedelta(days=days)).date()
            
            # Get insider transactions for ticker
            transactions_query = """
                SELECT 
                    COUNT(*) as transaction_count,
                    COUNT(DISTINCT insider_name) as unique_insiders,
                    SUM(transaction_value) as total_transaction_value,
                    AVG(transaction_value) as avg_transaction_value,
                    SUM(shares) as total_shares,
                    COUNT(CASE WHEN signal_generated = TRUE THEN 1 END) as signals_generated,
                    COUNT(CASE WHEN signal_quality = 'high' THEN 1 END) as high_quality_signals,
                    MAX(transaction_date) as latest_transaction_date
                FROM insider_transactions
                WHERE ticker = %s AND transaction_date >= %s
            """
            
            transactions_result = execute_query(transactions_query, (ticker, start_date), fetch_one=True)
            
            # Get LLM calls for ticker
            calls_query = """
                SELECT 
                    COUNT(*) as call_count,
                    COUNT(CASE WHEN is_closed = TRUE THEN 1 END) as closed_calls,
                    COUNT(CASE WHEN is_closed = FALSE THEN 1 END) as open_calls,
                    AVG(price_change_pct) as avg_price_change_pct,
                    SUM(pnl_dollars) as total_pnl,
                    AVG(pnl_dollars) as avg_pnl,
                    MAX(entry_date) as latest_call_date
                FROM llm_calls
                WHERE ticker = %s AND entry_date >= %s
            """
            
            calls_result = execute_query(calls_query, (ticker, start_date), fetch_one=True)
            
            # Get recent transactions
            recent_transactions_query = """
                SELECT * FROM insider_transactions
                WHERE ticker = %s AND transaction_date >= %s
                ORDER BY transaction_date DESC
                LIMIT 10
            """
            
            recent_transactions = execute_query(recent_transactions_query, (ticker, start_date))
            
            # Get recent calls
            recent_calls_query = """
                SELECT * FROM llm_calls
                WHERE ticker = %s AND entry_date >= %s
                ORDER BY entry_date DESC
                LIMIT 10
            """
            
            recent_calls = execute_query(recent_calls_query, (ticker, start_date))
            
            return {
                "ticker": ticker,
                "analysis": {
                    "insider_transactions": transactions_result,
                    "llm_calls": calls_result
                },
                "recent_transactions": recent_transactions,
                "recent_calls": recent_calls,
                "days": days,
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_ticker_analysis: {str(e)}")
            raise Exception(f"Failed to retrieve ticker analysis: {str(e)}")
    
    @mcp.tool()
    def get_signal_statistics(days: int = 30) -> Dict[str, Any]:
        """
        Get statistics on signal generation and quality.
        
        Args:
            days: Number of days to analyze (default: 30)
            
        Returns:
            Dict with signal statistics
        """
        try:
            start_date = (datetime.now() - timedelta(days=days)).date()
            
            query = """
                SELECT 
                    COUNT(*) as total_transactions,
                    COUNT(CASE WHEN signal_generated = TRUE THEN 1 END) as signals_generated,
                    COUNT(CASE WHEN signal_generated = FALSE THEN 1 END) as signals_pending,
                    COUNT(CASE WHEN alert_sent = TRUE THEN 1 END) as alerts_sent,
                    COUNT(CASE WHEN signal_quality = 'high' THEN 1 END) as high_quality_signals,
                    COUNT(CASE WHEN signal_quality = 'medium' THEN 1 END) as medium_quality_signals,
                    COUNT(CASE WHEN signal_quality = 'low' THEN 1 END) as low_quality_signals,
                    AVG(signal_score) as avg_signal_score,
                    AVG(final_signal_score) as avg_final_signal_score,
                    COUNT(CASE WHEN auto_rejected = TRUE THEN 1 END) as auto_rejected_signals
                FROM insider_transactions
                WHERE transaction_date >= %s
            """
            
            result = execute_query(query, (start_date,), fetch_one=True)
            
            # Calculate percentages
            total = result.get('total_transactions', 0) or 0
            signals = result.get('signals_generated', 0) or 0
            signal_rate = (signals / total * 100) if total > 0 else 0
            
            alerts = result.get('alerts_sent', 0) or 0
            alert_rate = (alerts / signals * 100) if signals > 0 else 0
            
            result['signal_generation_rate_pct'] = round(signal_rate, 2)
            result['alert_rate_pct'] = round(alert_rate, 2)
            
            return {
                "statistics": result,
                "days": days,
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_signal_statistics: {str(e)}")
            raise Exception(f"Failed to retrieve signal statistics: {str(e)}")
    
    @mcp.tool()
    def get_top_performers(
        days: int = 30,
        limit: int = 10,
        metric: str = "pnl"
    ) -> Dict[str, Any]:
        """
        Get top performing tickers or calls.
        
        Args:
            days: Number of days to analyze (default: 30)
            limit: Number of top performers to return (default: 10, max: 50)
            metric: Metric to rank by - 'pnl', 'price_change', or 'transaction_value' (default: 'pnl')
            
        Returns:
            Dict with top performers
        """
        try:
            if limit < 1 or limit > 50:
                limit = 10
            
            start_date = (datetime.now() - timedelta(days=days)).date()
            
            if metric == "pnl":
                query = """
                    SELECT 
                        ticker,
                        company_name,
                        COUNT(*) as call_count,
                        SUM(pnl_dollars) as total_pnl,
                        AVG(pnl_dollars) as avg_pnl,
                        AVG(price_change_pct) as avg_price_change_pct,
                        COUNT(CASE WHEN pnl_dollars > 0 THEN 1 END) as winning_calls
                    FROM llm_calls
                    WHERE entry_date >= %s AND is_closed = TRUE
                    GROUP BY ticker, company_name
                    ORDER BY total_pnl DESC NULLS LAST
                    LIMIT %s
                """
            elif metric == "price_change":
                query = """
                    SELECT 
                        ticker,
                        company_name,
                        COUNT(*) as call_count,
                        AVG(price_change_pct) as avg_price_change_pct,
                        SUM(pnl_dollars) as total_pnl
                    FROM llm_calls
                    WHERE entry_date >= %s AND is_closed = TRUE
                    GROUP BY ticker, company_name
                    ORDER BY avg_price_change_pct DESC NULLS LAST
                    LIMIT %s
                """
            else:  # transaction_value
                query = """
                    SELECT 
                        ticker,
                        company_name,
                        COUNT(*) as transaction_count,
                        SUM(transaction_value) as total_transaction_value,
                        AVG(transaction_value) as avg_transaction_value,
                        COUNT(DISTINCT insider_name) as unique_insiders
                    FROM insider_transactions
                    WHERE transaction_date >= %s
                    GROUP BY ticker, company_name
                    ORDER BY total_transaction_value DESC NULLS LAST
                    LIMIT %s
                """
            
            results = execute_query(query, (start_date, limit))
            
            return {
                "top_performers": results,
                "metric": metric,
                "days": days,
                "limit": limit,
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Error in get_top_performers: {str(e)}")
            raise Exception(f"Failed to retrieve top performers: {str(e)}")

