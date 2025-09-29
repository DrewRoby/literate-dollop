"""
Utility functions for working with Neo4j data types
"""
from datetime import datetime
from typing import Any, Optional
from neo4j.time import DateTime as Neo4jDateTime


def convert_neo4j_datetime(value: Any) -> Optional[datetime]:
    """
    Convert Neo4j DateTime to Python datetime
    
    Args:
        value: The value to convert (could be Neo4j DateTime, Python datetime, or None)
    
    Returns:
        Python datetime object or None
    """
    if value is None:
        return None
    
    # If it's already a Python datetime, return it
    if isinstance(value, datetime):
        return value
    
    # If it's a Neo4j DateTime, convert it
    if hasattr(value, 'to_native'):
        return value.to_native()
    
    # Try to parse string datetimes
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return None
    
    return None


def convert_neo4j_node(node_data: dict) -> dict:
    """
    Convert all Neo4j datetime fields in a node to Python datetimes
    
    Args:
        node_data: Dictionary containing node properties
    
    Returns:
        Dictionary with converted datetime values
    """
    converted = {}
    
    for key, value in node_data.items():
        if isinstance(value, Neo4jDateTime) or hasattr(value, 'to_native'):
            converted[key] = convert_neo4j_datetime(value)
        else:
            converted[key] = value
    
    return converted


def safe_get_datetime(node: Any, field_name: str) -> Optional[datetime]:
    """
    Safely extract and convert a datetime field from a Neo4j node
    
    Args:
        node: Neo4j node object
        field_name: Name of the datetime field
    
    Returns:
        Python datetime or None
    """
    value = node.get(field_name)
    return convert_neo4j_datetime(value)