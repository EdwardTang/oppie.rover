"""
Plan generator for creating task plans using LLM
"""
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

from .llm_client import LLMClient
from .config import PlannerConfig


logger = logging.getLogger(__name__)


class PlanGenerator:
    """Generates task plans using LLM"""
    
    def __init__(self, llm_client: LLMClient, config: PlannerConfig):
        self.llm_client = llm_client
        self.config = config
        
    def generate_plan(self, 
                     goal: str,
                     context: Dict[str, Any],
                     constraints: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], float]:
        """
        Generate a plan for the given goal.
        
        Args:
            goal: The task goal description
            context: Additional context (files, environment, previous results, etc.)
            constraints: Optional constraints (time limits, resource limits, etc.)
            
        Returns:
            Tuple of (plan_dict, confidence_score)
        """
        # Build the planning prompt
        prompt = self._build_planning_prompt(goal, context, constraints)
        
        # Generate plan using LLM
        try:
            response, tokens_used = self.llm_client.generate(
                prompt, 
                max_tokens=self.config.llm_config.max_tokens
            )
            
            # Parse the response
            plan, confidence = self._parse_plan_response(response)
            
            # Add metadata
            plan["metadata"] = {
                "generated_at": datetime.now().isoformat(),
                "tokens_used": tokens_used,
                "goal": goal,
                "planner_version": "0.1.0"
            }
            
            logger.info(f"Generated plan with {len(plan.get('steps', []))} steps, confidence: {confidence:.2f}")
            
            return plan, confidence
            
        except Exception as e:
            logger.error(f"Error generating plan: {e}")
            # Return a minimal error plan
            return {
                "steps": [{
                    "instruction": f"Failed to generate plan: {str(e)}",
                    "expected_outcome": "Error recovery needed",
                    "tool_use": [],
                }],
                "metadata": {
                    "error": str(e),
                    "generated_at": datetime.now().isoformat(),
                }
            }, 0.0
            
    def _build_planning_prompt(self, 
                              goal: str,
                              context: Dict[str, Any],
                              constraints: Optional[Dict[str, Any]] = None) -> str:
        """Build the prompt for plan generation"""
        prompt = f"""You are an expert software development planner. Create a detailed, step-by-step plan to achieve the following goal.

GOAL: {goal}

CONTEXT:
{self._format_context(context)}

{self._format_constraints(constraints) if constraints else ""}

Please create a comprehensive plan with the following requirements:
1. Break down the task into clear, atomic steps
2. Each step should have a specific instruction and expected outcome
3. Identify which tools or capabilities are needed for each step
4. Consider dependencies between steps
5. Include validation criteria where appropriate
6. Estimate your confidence (0.0-1.0) in the plan's success

Format your response as follows:

PLAN:
1. [First step instruction]
   Expected: [What should happen]
   Tools: [tool1, tool2]
   Validation: [How to verify success]

2. [Second step instruction]
   Expected: [What should happen]
   Tools: [tool1, tool2]
   Depends on: 1
   Validation: [How to verify success]

[Continue for all steps...]

CONFIDENCE: [0.0-1.0]

RATIONALE:
[Brief explanation of your approach and confidence level]
"""
        return prompt
        
    def _format_context(self, context: Dict[str, Any]) -> str:
        """Format context for the prompt"""
        formatted = []
        
        # Format different context types
        if "project_type" in context:
            formatted.append(f"Project Type: {context['project_type']}")
            
        if "current_files" in context:
            formatted.append(f"Current Files: {len(context['current_files'])} files")
            for file in context.get("current_files", [])[:5]:  # Show first 5
                formatted.append(f"  - {file}")
            if len(context.get("current_files", [])) > 5:
                formatted.append(f"  ... and {len(context['current_files']) - 5} more")
                
        if "environment" in context:
            formatted.append("Environment:")
            for key, value in context["environment"].items():
                formatted.append(f"  {key}: {value}")
                
        if "previous_results" in context:
            formatted.append(f"Previous Results Available: {len(context['previous_results'])} items")
            
        if "requirements" in context:
            formatted.append(f"Requirements: {context['requirements']}")
            
        if "technology_stack" in context:
            formatted.append(f"Technology Stack: {', '.join(context['technology_stack'])}")
            
        # Add any other context items
        for key, value in context.items():
            if key not in ["project_type", "current_files", "environment", 
                          "previous_results", "requirements", "technology_stack"]:
                if isinstance(value, (list, dict)):
                    formatted.append(f"{key}: {json.dumps(value, indent=2)}")
                else:
                    formatted.append(f"{key}: {value}")
                    
        return "\n".join(formatted)
        
    def _format_constraints(self, constraints: Dict[str, Any]) -> str:
        """Format constraints for the prompt"""
        if not constraints:
            return ""
            
        formatted = ["CONSTRAINTS:"]
        
        if "time_limit" in constraints:
            formatted.append(f"- Time limit: {constraints['time_limit']}")
            
        if "resource_limits" in constraints:
            formatted.append("- Resource limits:")
            for resource, limit in constraints["resource_limits"].items():
                formatted.append(f"  - {resource}: {limit}")
                
        if "required_capabilities" in constraints:
            formatted.append(f"- Required capabilities: {', '.join(constraints['required_capabilities'])}")
            
        if "forbidden_actions" in constraints:
            formatted.append(f"- Forbidden actions: {', '.join(constraints['forbidden_actions'])}")
            
        if "quality_requirements" in constraints:
            formatted.append("- Quality requirements:")
            for req in constraints["quality_requirements"]:
                formatted.append(f"  - {req}")
                
        return "\n".join(formatted) + "\n"
        
    def _parse_plan_response(self, response: str) -> Tuple[Dict[str, Any], float]:
        """Parse the LLM response into a structured plan"""
        plan = {"steps": []}
        confidence = 0.7  # Default confidence
        
        try:
            # Split response into sections
            sections = self._split_into_sections(response)
            
            # Parse plan steps
            if "PLAN" in sections:
                plan["steps"] = self._parse_plan_steps(sections["PLAN"])
                
            # Parse confidence
            if "CONFIDENCE" in sections:
                try:
                    confidence = float(sections["CONFIDENCE"].strip())
                    confidence = max(0.0, min(1.0, confidence))
                except ValueError:
                    logger.warning(f"Could not parse confidence: {sections['CONFIDENCE']}")
                    
            # Add rationale if present
            if "RATIONALE" in sections:
                plan["rationale"] = sections["RATIONALE"].strip()
                
        except Exception as e:
            logger.error(f"Error parsing plan response: {e}")
            # Create a single step plan from the response
            plan["steps"] = [{
                "instruction": response[:500],  # First 500 chars
                "expected_outcome": "To be determined",
                "tool_use": [],
            }]
            
        return plan, confidence
        
    def _split_into_sections(self, response: str) -> Dict[str, str]:
        """Split response into labeled sections"""
        sections = {}
        current_section = None
        current_content = []
        
        for line in response.split('\n'):
            # Check if this is a section header
            if line.strip() and line.strip().endswith(':') and line.strip()[:-1].upper() in ['PLAN', 'CONFIDENCE', 'RATIONALE']:
                # Save previous section
                if current_section:
                    sections[current_section] = '\n'.join(current_content).strip()
                    
                current_section = line.strip()[:-1].upper()
                current_content = []
            elif current_section:
                current_content.append(line)
                
        # Save last section
        if current_section:
            sections[current_section] = '\n'.join(current_content).strip()
            
        return sections
        
    def _parse_plan_steps(self, plan_text: str) -> List[Dict[str, Any]]:
        """Parse plan text into structured steps"""
        steps = []
        current_step = None
        
        lines = plan_text.strip().split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Check if it's a numbered step
            if line[0].isdigit() and '. ' in line:
                # Save previous step
                if current_step:
                    steps.append(current_step)
                    
                # Start new step
                parts = line.split('. ', 1)
                if len(parts) == 2:
                    current_step = {
                        "instruction": parts[1],
                        "expected_outcome": "",
                        "tool_use": [],
                        "validation": [],
                        "depends_on": [],
                        "metadata": {}
                    }
            elif current_step:
                # Parse step details
                if line.startswith('Expected:'):
                    current_step["expected_outcome"] = line[9:].strip()
                elif line.startswith('Tools:'):
                    tools = line[6:].strip().split(', ')
                    current_step["tool_use"] = [t.strip() for t in tools if t.strip()]
                elif line.startswith('Validation:'):
                    validation = line[11:].strip()
                    if validation:
                        current_step["validation"].append({
                            "type": "custom",
                            "criteria": validation
                        })
                elif line.startswith('Depends on:'):
                    deps = line[11:].strip().split(', ')
                    for dep in deps:
                        try:
                            dep_num = int(dep.strip()) - 1  # Convert to 0-based
                            if dep_num >= 0:
                                current_step["depends_on"].append(dep_num)
                        except ValueError:
                            pass
                            
        # Save last step
        if current_step:
            steps.append(current_step)
            
        return steps
        
    def enhance_plan_with_details(self, 
                                 plan: Dict[str, Any],
                                 additional_context: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance a plan with additional details and validations"""
        enhanced_plan = plan.copy()
        
        # Add default timeouts if not present
        for step in enhanced_plan.get("steps", []):
            if "timeout_seconds" not in step.get("metadata", {}):
                step.setdefault("metadata", {})["timeout_seconds"] = str(self.config.task_timeout_default)
                
        # Add semantic confidence if not present
        if "sem_confidence" not in enhanced_plan:
            enhanced_plan["sem_confidence"] = 0.8  # Default high confidence
            
        # Add required capabilities based on tool use
        capabilities = set()
        for step in enhanced_plan.get("steps", []):
            for tool in step.get("tool_use", []):
                if tool in ["file_write", "file_read", "file_delete"]:
                    capabilities.add("file_system")
                elif tool in ["http_request", "api_call"]:
                    capabilities.add("network")
                elif tool in ["run_command", "execute_script"]:
                    capabilities.add("process_execution")
                    
        enhanced_plan["required_capabilities"] = list(capabilities)
        
        return enhanced_plan 