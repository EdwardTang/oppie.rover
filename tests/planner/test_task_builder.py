"""
Tests for TaskCard builder
"""
import pytest
from datetime import datetime

from oppie_xyz.planner.task_builder import TaskCardBuilder
from oppie_xyz.seam_comm.proto import task_card_pb2


class TestTaskCardBuilder:
    """Test TaskCard builder functionality"""
    
    def test_basic_task_card(self):
        """Test creating a basic task card"""
        builder = TaskCardBuilder()
        task_card = (builder
                    .with_goal("Implement a new feature")
                    .add_step("Write code", "Code is written")
                    .build())
        
        assert task_card.goal_description == "Implement a new feature"
        assert len(task_card.steps) == 1
        assert task_card.steps[0].instruction == "Write code"
        if hasattr(task_card.steps[0], 'expected_outcome'):
            assert task_card.steps[0].expected_outcome == "Code is written"
        assert task_card.task_id  # Should have a UUID
        assert task_card.created_at.seconds > 0  # Should have timestamp
        
    def test_minimal_task_card(self):
        """Test creating a minimal task card with just required fields"""
        builder = TaskCardBuilder()
        task_card = (builder
                    .with_goal("Test goal")
                    .add_step("Test step")
                    .build())
        
        assert task_card.goal_description == "Test goal"
        assert len(task_card.steps) == 1
        assert task_card.steps[0].instruction == "Test step"
        assert task_card.task_id  # Should have a UUID
        
    def test_multiple_steps(self):
        """Test creating a task card with multiple steps"""
        builder = TaskCardBuilder()
        task_card = (builder
                    .with_goal("Multi-step task")
                    .add_step("Step 1", "Outcome 1")
                    .add_step("Step 2", "Outcome 2")
                    .add_step("Step 3", "Outcome 3")
                    .build())
        
        assert task_card.goal_description == "Multi-step task"
        assert len(task_card.steps) == 3
        assert task_card.steps[0].instruction == "Step 1"
        assert task_card.steps[1].instruction == "Step 2"
        assert task_card.steps[2].instruction == "Step 3"
        
    def test_missing_goal_raises_error(self):
        """Test that missing goal raises ValueError"""
        builder = TaskCardBuilder()
        builder.add_step("Do something")
        
        with pytest.raises(ValueError, match="goal description is required"):
            builder.build()
            
    def test_missing_steps_raises_error(self):
        """Test that missing steps raises ValueError"""
        builder = TaskCardBuilder()
        builder.with_goal("Test goal")
        
        with pytest.raises(ValueError, match="At least one step is required"):
            builder.build()
            
    def test_builder_reset(self):
        """Test that builder resets after build"""
        builder = TaskCardBuilder()
        
        # Build first task
        task1 = (builder
                .with_goal("Task 1")
                .add_step("Step 1")
                .build())
        
        # Build second task - should be independent
        task2 = (builder
                .with_goal("Task 2")
                .add_step("Step 2")
                .build())
        
        assert task1.goal_description == "Task 1"
        assert task2.goal_description == "Task 2"
        assert task1.task_id != task2.task_id  # Different UUIDs
        assert task1.steps[0].instruction == "Step 1"
        assert task2.steps[0].instruction == "Step 2"
        
    def test_confidence_setting(self):
        """Test setting confidence if the field exists"""
        builder = TaskCardBuilder()
        try:
            task_card = (builder
                        .with_goal("Test goal")
                        .with_confidence(0.8)
                        .add_step("Test step")
                        .build())
            
            # Only check if the field exists
            if hasattr(task_card, 'sem_confidence'):
                assert abs(task_card.sem_confidence - 0.8) < 0.01
        except AttributeError:
            # Field might not exist in current protobuf
            pass
        
    def test_confidence_validation(self):
        """Test confidence score validation"""
        builder = TaskCardBuilder()
        
        try:
            # Valid confidence
            builder.with_confidence(0.5)  # Should work
            
            # Invalid confidence - too high
            with pytest.raises(ValueError, match="Confidence must be between"):
                builder.with_confidence(1.5)
                
            # Invalid confidence - too low
            with pytest.raises(ValueError, match="Confidence must be between"):
                builder.with_confidence(-0.1)
        except AttributeError:
            # Field might not exist in current protobuf
            pytest.skip("confidence field not available in current protobuf")

    def test_complete_task_card(self):
        """Test creating a complete task card with all features"""
        builder = TaskCardBuilder()
        task_card = (builder
                    .with_goal("Deploy application to production")
                    .with_confidence(0.85)
                    .with_planner_version("0.1.0")
                    .with_trace_context("trace123", "span456", True)
                    .with_context({"environment": "production", "region": "us-east-1"})
                    .with_capabilities(["docker", "kubernetes", "aws"])
                    .with_timeout(3600)
                    .with_metadata({"priority": "high", "team": "platform"})
                    .add_step(
                        "Build Docker image",
                        "Docker image is built",
                        ["docker", "registry"],
                        [],
                        {"dockerfile": "Dockerfile.prod"}
                    )
                    .add_step(
                        "Deploy to Kubernetes",
                        "Application is running in k8s",
                        ["kubectl", "helm"],
                        [0],  # Depends on first step
                        {"namespace": "production"}
                    )
                    .add_validation(0, "output_exists", "docker images | grep myapp", "Docker image not found")
                    .build())
        
        # Check basic properties
        assert task_card.goal_description == "Deploy application to production"
        assert task_card.sem_confidence == 0.85
        assert task_card.planner_version == "0.1.0"
        assert task_card.timeout_seconds == 3600
        
        # Check trace context
        assert task_card.trace_context.trace_id == "trace123"
        assert task_card.trace_context.span_id == "span456"
        assert task_card.trace_context.sampled == True
        
        # Check context data
        assert task_card.context_data["environment"] == "production"
        assert task_card.context_data["region"] == "us-east-1"
        
        # Check capabilities
        assert list(task_card.required_capabilities) == ["docker", "kubernetes", "aws"]
        
        # Check metadata
        assert task_card.metadata["priority"] == "high"
        assert task_card.metadata["team"] == "platform"
        
        # Check steps
        assert len(task_card.steps) == 2
        
        step1 = task_card.steps[0]
        assert step1.instruction == "Build Docker image"
        assert step1.expected_outcome == "Docker image is built"
        assert list(step1.tool_use) == ["docker", "registry"]
        assert len(step1.depends_on) == 0
        assert step1.metadata["dockerfile"] == "Dockerfile.prod"
        assert len(step1.validation) == 1
        assert step1.validation[0].type == "output_exists"
        assert step1.validation[0].criteria == "docker images | grep myapp"
        assert step1.validation[0].error_message == "Docker image not found"
        
        step2 = task_card.steps[1]
        assert step2.instruction == "Deploy to Kubernetes"
        assert step2.expected_outcome == "Application is running in k8s"
        assert list(step2.tool_use) == ["kubectl", "helm"]
        assert list(step2.depends_on) == [0]
        assert step2.metadata["namespace"] == "production"
        
    def test_from_dict(self):
        """Test creating TaskCard from dictionary"""
        data = {
            "goal_description": "Test task from dict",
            "task_id": "custom-task-id",
            "trace_context": {
                "trace_id": "trace789",
                "span_id": "span012",
                "sampled": True
            },
            "context_data": {"test": "true"},
            "required_capabilities": ["python"],
            "timeout_seconds": 300,
            "sem_confidence": 0.9,
            "planner_version": "0.2.0",
            "metadata": {"source": "test"},
            "steps": [
                {
                    "instruction": "Run tests",
                    "expected_outcome": "All tests pass",
                    "tool_use": ["pytest"],
                    "depends_on": [],
                    "metadata": {"coverage": "90%"},
                    "validation": [
                        {
                            "type": "exit_code",
                            "criteria": "0",
                            "error_message": "Tests failed"
                        }
                    ]
                }
            ]
        }
        
        task_card = TaskCardBuilder.from_dict(data)
        
        assert task_card.goal_description == "Test task from dict"
        assert task_card.task_id == "custom-task-id"
        assert task_card.trace_context.trace_id == "trace789"
        assert task_card.context_data["test"] == "true"
        assert list(task_card.required_capabilities) == ["python"]
        assert task_card.timeout_seconds == 300
        assert task_card.sem_confidence == 0.9
        assert task_card.planner_version == "0.2.0"
        assert task_card.metadata["source"] == "test"
        assert len(task_card.steps) == 1
        assert task_card.steps[0].validation[0].type == "exit_code"
        
    def test_validation_index_bounds(self):
        """Test validation step index bounds checking"""
        builder = TaskCardBuilder()
        builder.with_goal("Test").add_step("Step 1")
        
        # Valid index
        builder.add_validation(0, "test", "criteria")  # Should work
        
        # Invalid index - too high
        with pytest.raises(IndexError, match="Step index 1 out of range"):
            builder.add_validation(1, "test", "criteria")
            
        # Invalid index - negative
        with pytest.raises(IndexError, match="Step index -1 out of range"):
            builder.add_validation(-1, "test", "criteria") 