"""
Main Planner/Reflector agent implementation
"""
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

from .config import PlannerConfig, LLMProvider
from .llm_client import LLMClientFactory, LLMClient
from .plan_generator import PlanGenerator
from .reflexion import ReflexionEngine
from .task_builder import TaskCardBuilder
from ..seam_comm.proto import task_card_pb2, result_card_pb2, error_card_pb2
from ..seam_comm.adapters import AdapterFactory
from ..seam_comm.telemetry import get_tracer, inject_trace_context


logger = logging.getLogger(__name__)
tracer = get_tracer(__name__)


class Planner:
    """
    Central orchestrator component that generates task plans using LLM,
    implements Reflexion mechanism, and handles TaskCard creation and dispatch.
    """
    
    def __init__(self, config: Optional[PlannerConfig] = None):
        self.config = config or PlannerConfig.default()
        
        # Initialize LLM client
        self.llm_client = self._create_llm_client()
        
        # Initialize components
        self.plan_generator = PlanGenerator(self.llm_client, self.config)
        self.reflexion_engine = ReflexionEngine(self.config.reflexion_config)
        
        # Initialize messaging adapter
        self.messaging_adapter = None
        self._init_messaging()
        
        # State tracking
        self.active_tasks: Dict[str, Dict[str, Any]] = {}
        self.completed_tasks: List[str] = []
        
        logger.info(f"Planner initialized with {self.config.llm_config.provider.value} provider")
        
    def _create_llm_client(self) -> LLMClient:
        """Create LLM client with fallback"""
        primary_config = self.config.llm_config
        
        # Create fallback config if primary is not Codex CLI
        fallback_config = None
        if primary_config.provider != LLMProvider.CODEX_CLI:
            fallback_config = primary_config.__class__.from_env(LLMProvider.CODEX_CLI)
            
        return LLMClientFactory.create_with_fallback(primary_config, fallback_config)
        
    def _init_messaging(self):
        """Initialize messaging adapter"""
        try:
            # Parse endpoint to get config
            endpoint_parts = self.config.messaging_endpoint.split("://")
            if len(endpoint_parts) > 1:
                config = {"server_address": self.config.messaging_endpoint}
            else:
                config = {}
                
            self.messaging_adapter = AdapterFactory.create_client(
                self.config.messaging_adapter,
                config
            )
            logger.info(f"Initialized {self.config.messaging_adapter} messaging adapter")
        except Exception as e:
            logger.warning(f"Failed to initialize messaging adapter: {e}")
            # Continue without messaging for testing
            self.messaging_adapter = None
            
    async def plan_task(self, 
                       goal: str,
                       context: Optional[Dict[str, Any]] = None,
                       constraints: Optional[Dict[str, Any]] = None) -> task_card_pb2.TaskCard:
        """
        Generate a plan for the given goal and create a TaskCard.
        
        Args:
            goal: The task goal description
            context: Additional context for planning
            constraints: Optional constraints
            
        Returns:
            TaskCard protobuf message ready for dispatch
        """
        with tracer.start_as_current_span("plan_task") as span:
            span.set_attribute("goal", goal)
            
            # Prepare context
            if context is None:
                context = {}
                
            # Add planner metadata to context
            context["planner_version"] = "0.1.0"
            context["timestamp"] = datetime.now().isoformat()
            
            # Generate initial plan
            logger.info(f"Generating plan for goal: {goal}")
            initial_plan, initial_confidence = self.plan_generator.generate_plan(
                goal, context, constraints
            )
            
            # Apply reflexion if enabled
            if self.config.reflexion_config.enable_circuit_breaker:
                logger.info("Applying reflexion mechanism")
                
                # Create LLM callback for reflexion
                def llm_callback(prompt: str, max_tokens: int) -> Tuple[str, int]:
                    return self.llm_client.generate(prompt, max_tokens)
                    
                reflexion_result = self.reflexion_engine.reflect(
                    initial_plan,
                    goal,
                    context,
                    llm_callback
                )
                
                final_plan = reflexion_result.final_plan
                final_confidence = reflexion_result.confidence_score
                
                # Log reflexion summary
                logger.info(f"Reflexion completed: {len(reflexion_result.rounds)} rounds, "
                          f"confidence: {final_confidence:.2f}, "
                          f"circuit breaker: {reflexion_result.circuit_breaker_triggered}")
                          
                if reflexion_result.circuit_breaker_triggered:
                    logger.warning(f"Circuit breaker triggered: {reflexion_result.trigger_reason}")
                    # TODO: Report to Emergence Regulator when available
                    
            else:
                final_plan = initial_plan
                final_confidence = initial_confidence
                
            # Enhance plan with additional details
            final_plan = self.plan_generator.enhance_plan_with_details(final_plan, context)
            
            # Create TaskCard
            task_card = self._create_task_card(goal, final_plan, final_confidence, context)
            
            # Track task
            self.active_tasks[task_card.task_id] = {
                "goal": goal,
                "plan": final_plan,
                "confidence": final_confidence,
                "created_at": datetime.now(),
                "status": "planned"
            }
            
            span.set_attribute("task_id", task_card.task_id)
            span.set_attribute("steps_count", len(task_card.steps))
            span.set_attribute("confidence", final_confidence)
            
            return task_card
            
    def _create_task_card(self, 
                         goal: str,
                         plan: Dict[str, Any],
                         confidence: float,
                         context: Dict[str, Any]) -> task_card_pb2.TaskCard:
        """Create a TaskCard from the plan"""
        builder = TaskCardBuilder()
        
        # Set basic properties
        builder.with_goal(goal)
        builder.with_confidence(confidence)
        builder.with_planner_version("0.1.0")
        
        # Add trace context if available
        trace_context = inject_trace_context()
        if trace_context:
            builder.with_trace_context(
                trace_context.trace_id,
                trace_context.span_id,
                trace_context.trace_flags
            )
            
        # Add context data
        context_data = {
            "project_type": context.get("project_type", "unknown"),
            "environment": context.get("environment", "development"),
        }
        builder.with_context(context_data)
        
        # Add capabilities
        if "required_capabilities" in plan:
            builder.with_capabilities(plan["required_capabilities"])
            
        # Set timeout
        timeout = plan.get("metadata", {}).get("timeout_seconds", self.config.task_timeout_default)
        builder.with_timeout(timeout)
        
        # Add metadata
        metadata = {
            "plan_confidence": str(confidence),
            "reflexion_rounds": str(len(self.reflexion_engine.rounds)),
            "llm_provider": self.config.llm_config.provider.value,
        }
        if "rationale" in plan:
            metadata["rationale"] = plan["rationale"]
        builder.with_metadata(metadata)
        
        # Add steps
        for i, step in enumerate(plan.get("steps", [])):
            builder.add_step(
                instruction=step["instruction"],
                expected_outcome=step.get("expected_outcome"),
                tool_use=step.get("tool_use", []),
                depends_on=step.get("depends_on", []),
                metadata=step.get("metadata", {})
            )
            
            # Add validations
            for validation in step.get("validation", []):
                builder.add_validation(
                    i,
                    validation.get("type", "custom"),
                    validation["criteria"],
                    validation.get("error_message")
                )
                
        return builder.build()
        
    async def dispatch_task(self, task_card: task_card_pb2.TaskCard) -> bool:
        """
        Dispatch a TaskCard to the Executor Pool via messaging.
        
        Returns:
            True if dispatch was successful
        """
        if not self.messaging_adapter:
            logger.warning("No messaging adapter available, cannot dispatch task")
            return False
            
        with tracer.start_as_current_span("dispatch_task") as span:
            span.set_attribute("task_id", task_card.task_id)
            
            try:
                # Send task card
                await self.messaging_adapter.send_task_card(task_card)
                
                # Update task status
                if task_card.task_id in self.active_tasks:
                    self.active_tasks[task_card.task_id]["status"] = "dispatched"
                    self.active_tasks[task_card.task_id]["dispatched_at"] = datetime.now()
                    
                logger.info(f"Dispatched task {task_card.task_id}")
                return True
                
            except Exception as e:
                logger.error(f"Failed to dispatch task {task_card.task_id}: {e}")
                span.record_exception(e)
                return False
                
    async def handle_result(self, result_card: result_card_pb2.ResultCard):
        """Handle a ResultCard from the Executor"""
        task_id = result_card.task_id
        
        with tracer.start_as_current_span("handle_result") as span:
            span.set_attribute("task_id", task_id)
            span.set_attribute("status", result_card.status)
            
            if task_id not in self.active_tasks:
                logger.warning(f"Received result for unknown task: {task_id}")
                return
                
            task_info = self.active_tasks[task_id]
            task_info["status"] = "completed"
            task_info["completed_at"] = datetime.now()
            task_info["result"] = result_card
            
            # Check if we need to trigger semantic negotiation
            if (result_card.status == result_card_pb2.ResultCard.PARTIAL and
                self.config.enable_semantic_negotiation):
                # TODO: Trigger semantic negotiation for unclear results
                logger.info(f"Task {task_id} returned partial result, may need clarification")
                
            # Move to completed
            self.completed_tasks.append(task_id)
            
            logger.info(f"Task {task_id} completed with status: {result_card.status}")
            
    async def handle_error(self, error_card: error_card_pb2.ErrorCard):
        """Handle an ErrorCard from the Executor"""
        task_id = error_card.task_id
        
        with tracer.start_as_current_span("handle_error") as span:
            span.set_attribute("task_id", task_id)
            span.set_attribute("error_category", error_card.category)
            
            if task_id not in self.active_tasks:
                logger.warning(f"Received error for unknown task: {task_id}")
                return
                
            task_info = self.active_tasks[task_id]
            task_info["status"] = "failed"
            task_info["failed_at"] = datetime.now()
            task_info["error"] = error_card
            
            # Determine if we should re-plan
            should_replan = self._should_replan_on_error(error_card)
            
            if should_replan:
                logger.info(f"Task {task_id} failed, attempting to re-plan")
                # Create new context with error information
                new_context = {
                    "previous_error": {
                        "category": error_card.category,
                        "message": error_card.error_message,
                        "task_id": task_id
                    },
                    "previous_plan": task_info["plan"]
                }
                
                # Re-plan the task
                new_task_card = await self.plan_task(
                    task_info["goal"],
                    new_context
                )
                
                # Dispatch the new plan
                await self.dispatch_task(new_task_card)
            else:
                logger.error(f"Task {task_id} failed with error: {error_card.error_message}")
                
    def _should_replan_on_error(self, error_card: error_card_pb2.ErrorCard) -> bool:
        """Determine if we should re-plan based on the error"""
        # Don't re-plan for certain error categories
        no_replan_categories = [
            error_card_pb2.ErrorCard.SANDBOX_VIOLATION,
            error_card_pb2.ErrorCard.CAPABILITY_MISSING,
        ]
        
        return error_card.category not in no_replan_categories
        
    async def start_message_listener(self):
        """Start listening for messages from Executors"""
        if not self.messaging_adapter:
            logger.warning("No messaging adapter, cannot start listener")
            return
            
        logger.info("Starting message listener")
        
        async def message_handler(message):
            """Handle incoming messages"""
            try:
                if isinstance(message, result_card_pb2.ResultCard):
                    await self.handle_result(message)
                elif isinstance(message, error_card_pb2.ErrorCard):
                    await self.handle_error(message)
                else:
                    logger.warning(f"Received unknown message type: {type(message)}")
            except Exception as e:
                logger.error(f"Error handling message: {e}")
                
        # Start listening
        await self.messaging_adapter.start_listening(message_handler)
        
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get the status of a task"""
        return self.active_tasks.get(task_id)
        
    def get_active_tasks(self) -> List[Dict[str, Any]]:
        """Get all active tasks"""
        return [
            {
                "task_id": task_id,
                "goal": info["goal"],
                "status": info["status"],
                "created_at": info["created_at"].isoformat(),
                "confidence": info["confidence"]
            }
            for task_id, info in self.active_tasks.items()
            if info["status"] not in ["completed", "failed"]
        ]
        
    def get_planner_stats(self) -> Dict[str, Any]:
        """Get planner statistics"""
        total_tasks = len(self.active_tasks)
        completed = sum(1 for t in self.active_tasks.values() if t["status"] == "completed")
        failed = sum(1 for t in self.active_tasks.values() if t["status"] == "failed")
        active = sum(1 for t in self.active_tasks.values() if t["status"] in ["planned", "dispatched"])
        
        return {
            "total_tasks": total_tasks,
            "completed_tasks": completed,
            "failed_tasks": failed,
            "active_tasks": active,
            "reflexion_stats": self.reflexion_engine.get_summary() if self.reflexion_engine.rounds else {},
            "llm_provider": self.config.llm_config.provider.value,
            "config": self.config.to_dict()
        } 