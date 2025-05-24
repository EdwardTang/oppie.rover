"""
Protobuf serialization/deserialization tools

Provides functionality for converting between Protobuf messages and Python dictionaries/JSON.
"""

import json
from typing import Dict, Any, Type
from google.protobuf.message import Message
from google.protobuf.json_format import MessageToDict, ParseDict

def protobuf_to_dict(message: Message) -> Dict[str, Any]:
    """Convert Protobuf message to dictionary
    
    Args:
        message: Protobuf message object
        
    Returns:
        Dict: Dictionary containing message fields
    """
    if message is None:
        return {}
    
    return MessageToDict(message, preserving_proto_field_name=True)

def dict_to_protobuf(data: Dict[str, Any], message_type: Type[Message]) -> Message:
    """Convert dictionary to Protobuf message
    
    Args:
        data: Dictionary data
        message_type: Protobuf message type
        
    Returns:
        Message: Protobuf message object
    """
    if not data:
        return message_type()
    
    message = message_type()
    ParseDict(data, message)
    return message

def protobuf_to_json(message: Message) -> str:
    """Convert Protobuf message to JSON string
    
    Args:
        message: Protobuf message object
        
    Returns:
        str: JSON string
    """
    if message is None:
        return "{}"
    
    return json.dumps(protobuf_to_dict(message))

def json_to_protobuf(json_str: str, message_type: Type[Message]) -> Message:
    """Convert JSON string to Protobuf message
    
    Args:
        json_str: JSON string
        message_type: Protobuf message type
        
    Returns:
        Message: Protobuf message object
    """
    if not json_str:
        return message_type()
    
    data = json.loads(json_str)
    return dict_to_protobuf(data, message_type) 