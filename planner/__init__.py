"""
Oppie.xyz Planner/Reflector Agent Module

Central orchestrator component that generates task plans using LLM,
implements Reflexion mechanism, and handles TaskCard creation and dispatch.
"""

from importlib import import_module

__version__ = "0.1.0"

__all__ = [
    "Planner",
    "ReflexionEngine",
    "PlanGenerator",
    "TaskCardBuilder",
]


def __getattr__(name: str):
    """Lazily import symbols to avoid heavy dependencies at import time."""
    if name == "Planner":
        return import_module(".planner", __package__).Planner
    if name == "ReflexionEngine":
        return import_module(".reflexion", __package__).ReflexionEngine
    if name == "PlanGenerator":
        return import_module(".plan_generator", __package__).PlanGenerator
    if name == "TaskCardBuilder":
        return import_module(".task_builder", __package__).TaskCardBuilder
    raise AttributeError(name)

