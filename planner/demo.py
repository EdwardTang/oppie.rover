"""
Demo CLI for testing Planner/Reflector Agent
"""
import asyncio
import argparse
import logging
import json
from typing import Dict, Any

from .planner import Planner
from .config import PlannerConfig, LLMProvider, LLMConfig, ReflexionConfig


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def demo_plan_task(planner: Planner, goal: str, context: Dict[str, Any] = None):
    """Demo planning a single task"""
    logger.info(f"Planning task: {goal}")
    
    # Generate plan
    task_card = await planner.plan_task(goal, context)
    
    # Display task card details
    print("\n" + "="*80)
    print(f"Task ID: {task_card.task_id}")
    print(f"Goal: {task_card.goal_description}")
    print(f"Confidence: {task_card.sem_confidence:.2f}")
    print(f"Timeout: {task_card.timeout_seconds}s")
    print(f"Required Capabilities: {', '.join(task_card.required_capabilities)}")
    
    print("\nSteps:")
    for i, step in enumerate(task_card.steps):
        print(f"\n{i+1}. {step.instruction}")
        if step.expected_outcome:
            print(f"   Expected: {step.expected_outcome}")
        if step.tool_use:
            print(f"   Tools: {', '.join(step.tool_use)}")
        if step.depends_on:
            print(f"   Depends on: {', '.join(map(str, step.depends_on))}")
        if step.validation:
            for v in step.validation:
                print(f"   Validation: {v.type} - {v.criteria}")
                
    print("\nMetadata:")
    for key, value in task_card.metadata.items():
        print(f"  {key}: {value}")
        
    print("="*80 + "\n")
    
    return task_card


async def demo_reflexion(planner: Planner):
    """Demo the reflexion mechanism"""
    goal = "Create a REST API with FastAPI that includes user authentication, database integration, and comprehensive testing"
    
    context = {
        "project_type": "web_api",
        "technology_stack": ["python", "fastapi", "postgresql", "pytest"],
        "requirements": "Must include JWT authentication, CRUD operations, and 90% test coverage",
        "environment": "development"
    }
    
    print("\n" + "="*80)
    print("REFLEXION DEMO")
    print("="*80)
    
    # Plan with reflexion enabled
    task_card = await demo_plan_task(planner, goal, context)
    
    # Show reflexion stats
    stats = planner.reflexion_engine.get_summary()
    if stats.get("total_rounds", 0) > 0:
        print("\nReflexion Summary:")
        print(f"  Total rounds: {stats['total_rounds']}")
        print(f"  Total tokens: {stats['total_tokens_used']}")
        print(f"  Duration: {stats['total_duration_seconds']:.2f}s")
        print(f"  Confidence scores: {stats['confidence_scores']}")
        print(f"  Final confidence: {stats['final_confidence']:.2f}")
        print(f"  Circuit breaker triggered: {stats['circuit_breaker_triggered']}")


async def demo_error_handling(planner: Planner):
    """Demo error handling scenarios"""
    print("\n" + "="*80)
    print("ERROR HANDLING DEMO")
    print("="*80)
    
    # Plan a task that might fail
    goal = "This is an intentionally vague and problematic task that should produce a low-confidence plan"
    
    task_card = await demo_plan_task(planner, goal)
    
    if task_card.sem_confidence < 0.5:
        print(f"\n⚠️  Low confidence plan detected: {task_card.sem_confidence:.2f}")
        print("The planner has identified potential issues with this task.")


async def demo_complex_task(planner: Planner):
    """Demo planning a complex multi-step task"""
    print("\n" + "="*80)
    print("COMPLEX TASK DEMO")
    print("="*80)
    
    goal = """
    Migrate a legacy monolithic application to microservices architecture:
    1. Analyze the current monolith and identify service boundaries
    2. Set up containerization with Docker
    3. Implement service discovery and API gateway
    4. Migrate data to separate databases
    5. Set up monitoring and logging
    6. Implement CI/CD pipeline
    7. Perform load testing and optimization
    """
    
    context = {
        "project_type": "migration",
        "current_stack": ["java", "spring", "mysql", "tomcat"],
        "target_stack": ["kubernetes", "docker", "postgresql", "redis", "rabbitmq"],
        "constraints": {
            "zero_downtime": True,
            "gradual_migration": True,
            "budget": "medium"
        }
    }
    
    await demo_plan_task(planner, goal, context)


async def interactive_demo(planner: Planner):
    """Interactive demo mode"""
    print("\n" + "="*80)
    print("INTERACTIVE PLANNER DEMO")
    print("="*80)
    print("Enter a goal for the planner (or 'quit' to exit)")
    print("Optionally provide context as JSON after the goal")
    
    while True:
        print("\n> ", end="", flush=True)
        user_input = input().strip()
        
        if user_input.lower() in ['quit', 'exit', 'q']:
            break
            
        if not user_input:
            continue
            
        # Check if there's JSON context
        parts = user_input.split('\n', 1)
        goal = parts[0]
        context = {}
        
        if len(parts) > 1:
            try:
                context = json.loads(parts[1])
            except json.JSONDecodeError:
                print("Warning: Invalid JSON context, proceeding without context")
                
        try:
            await demo_plan_task(planner, goal, context)
        except Exception as e:
            print(f"Error: {e}")


async def main():
    parser = argparse.ArgumentParser(description="Planner/Reflector Agent Demo")
    parser.add_argument("--provider", choices=["openai", "gemini", "codex_cli"], 
                       default="codex_cli", help="LLM provider to use")
    parser.add_argument("--model", help="Model name override")
    parser.add_argument("--api-key", help="API key (for OpenAI/Gemini)")
    parser.add_argument("--no-reflexion", action="store_true", help="Disable reflexion")
    parser.add_argument("--mode", choices=["basic", "reflexion", "error", "complex", "interactive", "all"],
                       default="all", help="Demo mode to run")
    
    args = parser.parse_args()
    
    # Create configuration
    provider_map = {
        "openai": LLMProvider.OPENAI,
        "gemini": LLMProvider.GEMINI,
        "codex_cli": LLMProvider.CODEX_CLI
    }
    
    llm_config = LLMConfig.from_env(provider_map[args.provider])
    if args.model:
        llm_config.model = args.model
    if args.api_key:
        llm_config.api_key = args.api_key
        
    reflexion_config = ReflexionConfig(enable_circuit_breaker=not args.no_reflexion)
    
    config = PlannerConfig(
        llm_config=llm_config,
        reflexion_config=reflexion_config,
        messaging_adapter="nats",  # Demo uses NATS
        enable_tracing=False  # Disable tracing for demo
    )
    
    # Create planner
    planner = Planner(config)
    
    print(f"Planner initialized with {config.llm_config.provider.value} provider")
    print(f"Model: {config.llm_config.model}")
    print(f"Reflexion: {'enabled' if not args.no_reflexion else 'disabled'}")
    
    # Run demos based on mode
    if args.mode == "all":
        # Basic demo
        goal = "Create a Python CLI tool that converts CSV files to JSON format"
        context = {"project_type": "cli_tool", "language": "python"}
        await demo_plan_task(planner, goal, context)
        
        # Run other demos if not disabled
        if not args.no_reflexion:
            await demo_reflexion(planner)
        await demo_error_handling(planner)
        await demo_complex_task(planner)
        
    elif args.mode == "basic":
        goal = "Create a Python CLI tool that converts CSV files to JSON format"
        context = {"project_type": "cli_tool", "language": "python"}
        await demo_plan_task(planner, goal, context)
        
    elif args.mode == "reflexion":
        await demo_reflexion(planner)
        
    elif args.mode == "error":
        await demo_error_handling(planner)
        
    elif args.mode == "complex":
        await demo_complex_task(planner)
        
    elif args.mode == "interactive":
        await interactive_demo(planner)
        
    # Show final stats
    stats = planner.get_planner_stats()
    print("\nPlanner Statistics:")
    print(f"  Total tasks: {stats['total_tasks']}")
    print(f"  Active tasks: {stats['active_tasks']}")
    print(f"  LLM provider: {stats['llm_provider']}")


if __name__ == "__main__":
    asyncio.run(main()) 