"""
Reflexion mechanism implementation for self-review and plan refinement
"""
import time
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime

from .config import ReflexionConfig


logger = logging.getLogger(__name__)


@dataclass
class ReflexionRound:
    """Represents a single round of reflexion"""
    round_number: int
    input_plan: Dict[str, Any]
    reflection: str
    refined_plan: Dict[str, Any]
    confidence_score: float
    tokens_used: int
    duration_seconds: float
    timestamp: datetime


@dataclass
class ReflexionResult:
    """Result of the reflexion process"""
    final_plan: Dict[str, Any]
    rounds: List[ReflexionRound]
    total_tokens_used: int
    total_duration_seconds: float
    circuit_breaker_triggered: bool
    trigger_reason: Optional[str] = None
    confidence_score: float = 0.0


class ReflexionEngine:
    """
    Implements the Reflexion mechanism for self-review and plan refinement.
    
    The engine allows the Planner to iterate up to 3 reflection cycles within
    token/time budget (e.g., 8000 tokens or 90 seconds) to refine plans or catch errors.
    """
    
    def __init__(self, config: ReflexionConfig):
        self.config = config
        self.rounds: List[ReflexionRound] = []
        self._start_time: Optional[float] = None
        self._total_tokens_used: int = 0
        
    def reflect(self, 
                initial_plan: Dict[str, Any],
                goal: str,
                context: Dict[str, Any],
                llm_callback) -> ReflexionResult:
        """
        Execute the reflexion process on a plan.
        
        Args:
            initial_plan: The initial plan to reflect on
            goal: The original goal/task description
            context: Additional context for reflection
            llm_callback: Callable that takes (prompt, max_tokens) and returns (response, tokens_used)
            
        Returns:
            ReflexionResult with the final refined plan
        """
        self._start_time = time.time()
        self._total_tokens_used = 0
        self.rounds = []
        
        current_plan = initial_plan
        circuit_breaker_triggered = False
        trigger_reason = None
        
        for round_num in range(1, self.config.max_rounds + 1):
            # Check budget constraints before starting round
            elapsed_time = time.time() - self._start_time
            if elapsed_time >= self.config.timeout_seconds:
                circuit_breaker_triggered = True
                trigger_reason = f"Timeout exceeded: {elapsed_time:.1f}s >= {self.config.timeout_seconds}s"
                logger.warning(f"Reflexion circuit breaker: {trigger_reason}")
                break
                
            if self._total_tokens_used >= self.config.max_tokens:
                circuit_breaker_triggered = True
                trigger_reason = f"Token limit exceeded: {self._total_tokens_used} >= {self.config.max_tokens}"
                logger.warning(f"Reflexion circuit breaker: {trigger_reason}")
                break
            
            # Perform reflection round
            round_result = self._reflect_round(
                round_num,
                current_plan,
                goal,
                context,
                llm_callback
            )
            
            self.rounds.append(round_result)
            self._total_tokens_used += round_result.tokens_used
            
            # Check if we've reached satisfactory confidence
            if round_result.confidence_score >= self.config.confidence_threshold:
                logger.info(f"Reflexion completed with confidence {round_result.confidence_score:.2f} after {round_num} rounds")
                current_plan = round_result.refined_plan
                break
                
            # Update plan for next round
            current_plan = round_result.refined_plan
            
        # Calculate final confidence score
        final_confidence = self.rounds[-1].confidence_score if self.rounds else 0.0
        
        return ReflexionResult(
            final_plan=current_plan,
            rounds=self.rounds,
            total_tokens_used=self._total_tokens_used,
            total_duration_seconds=time.time() - self._start_time,
            circuit_breaker_triggered=circuit_breaker_triggered,
            trigger_reason=trigger_reason,
            confidence_score=final_confidence
        )
    
    def _reflect_round(self,
                       round_number: int,
                       plan: Dict[str, Any],
                       goal: str,
                       context: Dict[str, Any],
                       llm_callback) -> ReflexionRound:
        """Execute a single round of reflection"""
        round_start = time.time()
        
        # Construct reflection prompt
        prompt = self._build_reflection_prompt(round_number, plan, goal, context)
        
        # Calculate available tokens for this round
        remaining_tokens = self.config.max_tokens - self._total_tokens_used
        max_tokens_for_round = min(3000, remaining_tokens)  # Cap at 3000 per round
        
        # Call LLM for reflection
        try:
            response, tokens_used = llm_callback(prompt, max_tokens_for_round)
            
            # Parse reflection response
            reflection_text, refined_plan, confidence = self._parse_reflection_response(response, plan)
            
        except Exception as e:
            logger.error(f"Error in reflexion round {round_number}: {e}")
            # Return original plan with low confidence on error
            reflection_text = f"Error during reflection: {str(e)}"
            refined_plan = plan
            confidence = 0.0
            tokens_used = 0
            
        round_duration = time.time() - round_start
        
        return ReflexionRound(
            round_number=round_number,
            input_plan=plan,
            reflection=reflection_text,
            refined_plan=refined_plan,
            confidence_score=confidence,
            tokens_used=tokens_used,
            duration_seconds=round_duration,
            timestamp=datetime.now()
        )
    
    def _build_reflection_prompt(self,
                                round_number: int,
                                plan: Dict[str, Any],
                                goal: str,
                                context: Dict[str, Any]) -> str:
        """Build the prompt for reflection"""
        prompt = f"""You are reviewing and refining a task plan (Reflexion Round {round_number}/{self.config.max_rounds}).

GOAL: {goal}

CURRENT PLAN:
{self._format_plan(plan)}

CONTEXT:
{self._format_context(context)}

Please analyze this plan and provide:
1. REFLECTION: Critical analysis of the plan's strengths and weaknesses
2. ISSUES: Any errors, missing steps, or potential problems
3. REFINEMENTS: Specific improvements to make the plan more robust
4. CONFIDENCE: Your confidence score (0.0-1.0) that the refined plan will succeed

Format your response as:
REFLECTION:
[Your analysis]

ISSUES:
[List any problems]

REFINED_PLAN:
[Updated plan in the same format]

CONFIDENCE: [0.0-1.0]
"""
        
        # Add previous round reflections if available
        if round_number > 1 and self.rounds:
            previous_reflections = "\n\nPREVIOUS REFLECTIONS:\n"
            for prev_round in self.rounds[-2:]:  # Last 2 rounds
                previous_reflections += f"\nRound {prev_round.round_number}: {prev_round.reflection[:200]}..."
            prompt = prompt.replace("CONTEXT:", f"CONTEXT:\n{previous_reflections}\n")
            
        return prompt
    
    def _format_plan(self, plan: Dict[str, Any]) -> str:
        """Format plan for display in prompt"""
        formatted = []
        for i, step in enumerate(plan.get("steps", [])):
            formatted.append(f"{i+1}. {step.get('instruction', 'No instruction')}")
            if "expected_outcome" in step:
                formatted.append(f"   Expected: {step['expected_outcome']}")
            if "tool_use" in step:
                formatted.append(f"   Tools: {', '.join(step['tool_use'])}")
        return "\n".join(formatted)
    
    def _format_context(self, context: Dict[str, Any]) -> str:
        """Format context for display in prompt"""
        formatted = []
        for key, value in context.items():
            if isinstance(value, (list, dict)):
                formatted.append(f"{key}: {len(value)} items")
            else:
                formatted.append(f"{key}: {value}")
        return "\n".join(formatted)
    
    def _parse_reflection_response(self, 
                                  response: str,
                                  original_plan: Dict[str, Any]) -> Tuple[str, Dict[str, Any], float]:
        """Parse the LLM reflection response"""
        # Default values
        reflection_text = ""
        refined_plan = original_plan.copy()
        confidence = 0.5
        
        try:
            # Extract sections from response
            sections = {}
            current_section = None
            current_content = []
            
            for line in response.split('\n'):
                if line.strip().endswith(':') and line.strip()[:-1] in ['REFLECTION', 'ISSUES', 'REFINED_PLAN', 'CONFIDENCE']:
                    if current_section:
                        sections[current_section] = '\n'.join(current_content).strip()
                    current_section = line.strip()[:-1]
                    current_content = []
                elif current_section:
                    current_content.append(line)
                    
            if current_section:
                sections[current_section] = '\n'.join(current_content).strip()
            
            # Extract reflection text
            reflection_text = sections.get('REFLECTION', '') + '\n' + sections.get('ISSUES', '')
            
            # Extract confidence score
            if 'CONFIDENCE' in sections:
                try:
                    confidence = float(sections['CONFIDENCE'].strip())
                    confidence = max(0.0, min(1.0, confidence))  # Clamp to [0, 1]
                except ValueError:
                    logger.warning(f"Could not parse confidence score: {sections['CONFIDENCE']}")
            
            # Parse refined plan if provided
            if 'REFINED_PLAN' in sections:
                refined_plan = self._parse_plan_text(sections['REFINED_PLAN'])
                if not refined_plan.get('steps'):
                    refined_plan = original_plan.copy()
                    
        except Exception as e:
            logger.error(f"Error parsing reflection response: {e}")
            
        return reflection_text.strip(), refined_plan, confidence
    
    def _parse_plan_text(self, plan_text: str) -> Dict[str, Any]:
        """Parse plan text into structured format"""
        plan = {"steps": []}
        
        lines = plan_text.strip().split('\n')
        current_step = None
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Check if it's a numbered step
            if line[0].isdigit() and '. ' in line:
                if current_step:
                    plan["steps"].append(current_step)
                    
                # Extract step number and instruction
                parts = line.split('. ', 1)
                if len(parts) == 2:
                    current_step = {
                        "instruction": parts[1],
                        "tool_use": [],
                        "metadata": {}
                    }
            elif current_step and line.startswith('Expected:'):
                current_step["expected_outcome"] = line[9:].strip()
            elif current_step and line.startswith('Tools:'):
                tools = line[6:].strip().split(', ')
                current_step["tool_use"] = [t.strip() for t in tools if t.strip()]
                
        if current_step:
            plan["steps"].append(current_step)
            
        return plan
    
    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of the reflexion process"""
        return {
            "total_rounds": len(self.rounds),
            "total_tokens_used": self._total_tokens_used,
            "total_duration_seconds": time.time() - self._start_time if self._start_time else 0,
            "confidence_scores": [r.confidence_score for r in self.rounds],
            "final_confidence": self.rounds[-1].confidence_score if self.rounds else 0.0,
            "circuit_breaker_triggered": any(r.tokens_used == 0 for r in self.rounds),  # Simplified check
        } 