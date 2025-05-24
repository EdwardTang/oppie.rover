"""
TaskCard builder for creating task cards with fluent interface
"""
import uuid
from typing import Dict, List, Any, Optional
from datetime import datetime
from google.protobuf.timestamp_pb2 import Timestamp

# Import protobuf definitions
from ..seam_comm.proto import task_card_pb2, common_pb2


class TaskCardBuilder:
    """Builder for creating TaskCard protobuf messages"""
    
    def __init__(self):
        self._reset()
        
    def _reset(self):
        """Reset the builder state"""
        self._task_card = task_card_pb2.TaskCard()
        self._task_card.task_id = str(uuid.uuid4())
        
        # Set creation timestamp
        timestamp = Timestamp()
        timestamp.FromDatetime(datetime.now())
        self._task_card.created_at.CopyFrom(timestamp)
        
    def with_goal(self, goal: str) -> "TaskCardBuilder":
        """Set the task goal description"""
        self._task_card.goal_description = goal
        return self
        
    def with_confidence(self, confidence: float) -> "TaskCardBuilder":
        """Set the semantic confidence score"""
        if not 0.0 <= confidence <= 1.0:
            raise ValueError("Confidence must be between 0.0 and 1.0")
        self._task_card.sem_confidence = confidence
        return self
        
    def with_planner_version(self, version: str) -> "TaskCardBuilder":
        """Set planner version information"""
        self._task_card.planner_version = version
        return self
        
    def with_trace_context(self, trace_id: str, span_id: str, sampled: bool = True) -> "TaskCardBuilder":
        """Add OpenTelemetry trace context"""
        self._task_card.trace_context.trace_id = trace_id
        self._task_card.trace_context.span_id = span_id
        self._task_card.trace_context.sampled = sampled
        return self
        
    def with_context(self, context_data: Dict[str, Any]) -> "TaskCardBuilder":
        """Add context data"""
        for key, value in context_data.items():
            self._task_card.context_data[key] = str(value)
        return self
        
    def with_capabilities(self, capabilities: List[str]) -> "TaskCardBuilder":
        """Set required capabilities"""
        self._task_card.required_capabilities[:] = capabilities
        return self
        
    def with_timeout(self, timeout_seconds: int) -> "TaskCardBuilder":
        """Set task timeout in seconds"""
        self._task_card.timeout_seconds = timeout_seconds
        return self
        
    def with_metadata(self, metadata: Dict[str, Any]) -> "TaskCardBuilder":
        """Add metadata"""
        for key, value in metadata.items():
            self._task_card.metadata[key] = str(value)
        return self
        
    def add_step(self, 
                instruction: str,
                expected_outcome: Optional[str] = None,
                tool_use: Optional[List[str]] = None,
                depends_on: Optional[List[int]] = None,
                metadata: Optional[Dict[str, Any]] = None) -> "TaskCardBuilder":
        """Add a step to the task card"""
        step = self._task_card.steps.add()
        step.instruction = instruction
        
        if expected_outcome:
            step.expected_outcome = expected_outcome
            
        if tool_use:
            step.tool_use[:] = tool_use
            
        if depends_on:
            step.depends_on[:] = depends_on
            
        if metadata:
            for key, value in metadata.items():
                step.metadata[key] = str(value)
                
        return self
        
    def add_validation(self,
                      step_index: int,
                      validation_type: str,
                      criteria: str,
                      error_message: Optional[str] = None) -> "TaskCardBuilder":
        """Add validation criteria to a specific step"""
        if not 0 <= step_index < len(self._task_card.steps):
            raise IndexError(f"Step index {step_index} out of range")
            
        validation = self._task_card.steps[step_index].validation.add()
        validation.type = validation_type
        validation.criteria = criteria
        if error_message:
            validation.error_message = error_message
            
        return self
        
    def build(self) -> task_card_pb2.TaskCard:
        """Build and return the TaskCard"""
        if not self._task_card.goal_description:
            raise ValueError("Task goal description is required")
            
        if not self._task_card.steps:
            raise ValueError("At least one step is required")
            
        # Create a copy of the task card
        result = task_card_pb2.TaskCard()
        result.CopyFrom(self._task_card)
        
        # Reset for next use
        self._reset()
        
        return result
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> task_card_pb2.TaskCard:
        """Create a TaskCard from a dictionary representation"""
        builder = cls()
        
        # Set basic properties
        if "goal_description" in data:
            builder.with_goal(data["goal_description"])
            
        if "task_id" in data:
            builder._task_card.task_id = data["task_id"]
            
        if "sem_confidence" in data:
            builder.with_confidence(data["sem_confidence"])
            
        if "planner_version" in data:
            builder.with_planner_version(data["planner_version"])
            
        # Set trace context
        if "trace_context" in data:
            tc = data["trace_context"]
            builder.with_trace_context(
                tc.get("trace_id", ""),
                tc.get("span_id", ""),
                tc.get("sampled", True)
            )
            
        # Set context data
        if "context_data" in data:
            builder.with_context(data["context_data"])
            
        # Set capabilities
        if "required_capabilities" in data:
            builder.with_capabilities(data["required_capabilities"])
            
        # Set timeout
        if "timeout_seconds" in data:
            builder.with_timeout(data["timeout_seconds"])
            
        # Set metadata
        if "metadata" in data:
            builder.with_metadata(data["metadata"])
            
        # Add steps
        for step_data in data.get("steps", []):
            builder.add_step(
                step_data["instruction"],
                step_data.get("expected_outcome"),
                step_data.get("tool_use", []),
                step_data.get("depends_on", []),
                step_data.get("metadata", {})
            )
            
            # Add validations
            for i, validation in enumerate(step_data.get("validation", [])):
                builder.add_validation(
                    len(builder._task_card.steps) - 1,  # Current step index
                    validation["type"],
                    validation["criteria"],
                    validation.get("error_message")
                )
                
        return builder.build() 