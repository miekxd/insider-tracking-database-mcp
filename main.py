import os
import logging
import traceback
from typing import Dict, Any
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from datetime import datetime

from db import init_db_pool, close_db_pool, test_connection
from insider_transactions import register_insider_transaction_tools
from llm_calls import register_llm_calls_tools
from market_context import register_market_context_tools
from analytics import register_analytics_tools

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastMCP server (stateless for simple requests)
port = int(os.getenv("PORT", 8000))
mcp = FastMCP("investment-tracking-mcp", port=port, stateless_http=True, host="0.0.0.0")

def validate_api_key(api_key: str = None) -> bool:
    """
    Validate API key for production deployment.
    Returns True if valid or if running in local development mode.
    """
    # Skip auth in local development (when no API_KEY is set)
    required_api_key = os.getenv("API_KEY")
    if not required_api_key:
        return True  # Local development mode
    
    return api_key == required_api_key

@mcp.tool()
def health_check() -> Dict[str, Any]:
    """
    Health check endpoint for Railway deployment and monitoring.
    Tests database connection and returns server status.
    
    Returns:
        Dict with health status, timestamp, and basic server info
    """
    try:
        db_status = "connected" if test_connection() else "disconnected"
        
        return {
            "status": "healthy" if db_status == "connected" else "degraded",
            "timestamp": datetime.now().isoformat(),
            "server": "investment-tracking-mcp",
            "port": port,
            "database": db_status,
            "version": "1.0.0"
        }
    except Exception as e:
        logger.error(f"Health check error: {str(e)}")
        return {
            "status": "unhealthy",
            "timestamp": datetime.now().isoformat(),
            "server": "investment-tracking-mcp",
            "port": port,
            "database": "error",
            "error": str(e),
            "version": "1.0.0"
        }

# Register all tool modules
register_insider_transaction_tools(mcp)
register_llm_calls_tools(mcp)
register_market_context_tools(mcp)
register_analytics_tools(mcp)

if __name__ == "__main__":
    try:
        # Initialize database connection pool
        logger.info("Initializing database connection pool...")
        init_db_pool()
        
        if test_connection():
            logger.info("Database connection successful")
        else:
            logger.warning("Database connection test failed - server will start but queries may fail")
        
        logger.info("Starting Investment Tracking MCP server...")
        logger.info(f"Server configured for port: {port}")
        
        # Get transport mode from environment variable
        transport_mode = os.getenv("MCP_TRANSPORT", "stdio")
        
        # Check if API key is configured
        api_key_configured = bool(os.getenv("API_KEY"))
        logger.info(f"API key authentication: {'enabled' if api_key_configured else 'disabled (local development)'}")
        
        if transport_mode == "http":
            # SSE transport (FastMCP's HTTP implementation)
            logger.info("Starting MCP server with SSE transport...")
            mcp.run(transport='sse')
        else:
            # Stdio for local development
            logger.info("Starting stdio server for local development")
            mcp.run(transport='stdio')
            
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
        close_db_pool()
    except Exception as e:
        logger.error(f"Failed to start MCP server: {str(e)}")
        logger.error("Please check your configuration and try again.")
        traceback.print_exc()
        close_db_pool()
        exit(1)
