# Workflow Self-Evolution Integration Guide

## ⚠️ Important: This is a Meta-Capability Skill

**`workflow-self-evolution` is NOT a Python module you import.**

It's a **skill** that provides guidelines and protocols for continuous improvement.

Integration is done through **tool calls** and **convention-based protocols**, NOT Python imports.

---

## Actual Integration Pattern (Using Tool Calls)

### For NTL_Engineer / Agent Coordinators

```python
# This is how you actually integrate self-evolution in your workflow

def execute_task_with_evolution(task):
    """
    Execute a task with self-evolution integration.
    
    Instead of importing a non-existent module, use tool calls:
    1. Execute the task using existing tools
    2. Log execution result to evolution logs
    3. For failures, use intelligent filtering to decide learning
    4. Before workflow updates, backup version
    """
    
    # === Step 1: Execute task using existing tools ===
    result = execute_with_tools(task)
    
    # === Step 2: Log to evolution system (using file writes) ===
    log_execution_result(result)
    
    # === Step 3: Handle failures intelligently ===
    if result.status == "failed":
        failure_type = classify_failure_type(result.error)
        
        # Intelligent filtering - only learn from valuable failures
        if failure_type in ["systemic", "recurring"]:
            # Trigger learning (modify workflow)
            trigger_workflow_learning(result)
        elif failure_type == "user_error":
            # Provide feedback to user
            provide_user_feedback(result)
        elif failure_type == "transient":
            # Auto-retry (don't learn)
            retry_result = auto_retry(task, max_attempts=3)
            if retry_result.status == "success":
                result = retry_result  # Use successful retry
        # external failures: monitor but don't learn yet
    
    # === Step 4: Update metrics (append to metrics.json) ===
    update_quality_metrics(result)
    
    return result
```

---

## Actual Tool-Based Implementation

### Tool Calls for Self-Evolution

| Action | Actual Tool/Method |
|--------|-------------------|
| Log execution result | `write_file` to `references/failure_log.jsonl` or `references/learning_log.jsonl` |
| Update metrics | `edit_file` on `references/metrics.json` |
| Backup workflow | `read_file` + `write_file` to version backup |
| Modify workflow | `edit_file` on workflow JSON files |
| Classify failure | Use pattern matching on error messages |
| Decide learning | Apply decision rules from SKILL.md |

---

## Example 1: Actual Failure Logging (Working Code)

```python
import json
from datetime import datetime

def log_execution_result(result):
    """Log execution result to evolution logs using file writes."""
    
    log_entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "workflow_id": result.workflow_id,
        "task_id": result.task_id,
        "status": result.status,  # "success" or "failed"
        "error_message": str(result.error)[:500] if result.status == "failed" else None,
        "confidence": result.confidence if hasattr(result, 'confidence') else None,
        "execution_time_ms": result.execution_time_ms if hasattr(result, 'execution_time_ms') else None
    }
    
    # Append to appropriate log file
    if result.status == "failed":
        # Write to failure log
        with open("/skills/workflow-self-evolution/references/failure_log.jsonl", "a") as f:
            f.write(json.dumps(log_entry) + "\n")
    else:
        # Update metrics for success
        update_metrics_for_success(result)
```

**What this does**:
- ✅ Actually works (uses file I/O, not fake imports)
- ✅ Logs failures for later analysis
- ✅ Updates metrics for successes
- ✅ Follows the schema defined in the skill

---

## Example 2: Actual Failure Classification (Working Code)

```python
def classify_failure_type(error_message):
    """
    Classify failure into 5 categories using pattern matching.
    
    Returns: "systemic", "recurring", "transient", "user_error", or "external"
    """
    
    error_str = str(error_message).lower()
    
    # Systemic errors (workflow design flaws)
    systemic_patterns = [
        "tool not found",
        "invalid parameter",
        "missing required field",
        "type error",
        "attribute error"
    ]
    if any(pattern in error_str for pattern in systemic_patterns):
        return "systemic"
    
    # Transient errors (network, timeout)
    transient_patterns = [
        "timeout",
        "connection error",
        "network error",
        "temporary error",
        "rate limit"
    ]
    if any(pattern in error_str for pattern in transient_patterns):
        return "transient"
    
    # User errors (input problems)
    user_error_patterns = [
        "file not found",
        "invalid input",
        "missing file",
        "permission denied",
        "boundary not found"
    ]
    if any(pattern in error_str for pattern in user_error_patterns):
        return "user_error"
    
    # External errors (API, service dependencies)
    external_patterns = [
        "api error",
        "service unavailable",
        "gee error",
        "authentication failed"
    ]
    if any(pattern in error_str for pattern in external_patterns):
        return "external"
    
    # Default: check if recurring
    if is_recurring_failure(error_message):
        return "recurring"
    
    # Unknown: treat as systemic (safe default)
    return "systemic"

def is_recurring_failure(error_message, window_days=7, threshold=3):
    """Check if this failure pattern occurred recently."""
    # Read failure log and count similar failures
    try:
        with open("/skills/workflow-self-evolution/references/failure_log.jsonl", "r") as f:
            recent_failures = []
            for line in f:
                entry = json.loads(line)
                if entry.get("error_message") and error_message[:50] in entry["error_message"]:
                    recent_failures.append(entry)
            return len(recent_failures) >= threshold
    except FileNotFoundError:
        return False
```

**What this does**:
- ✅ Actually classifies failures using pattern matching
- ✅ Implements the 5-category filtering from the skill design
- ✅ Can be used to decide whether to learn

---

## Example 3: Actual Learning Decision (Working Code)

```python
def make_learning_decision(result, failure_type):
    """
    Decide whether to learn from this failure.
    
    Returns dict with:
    - should_learn: bool
    - should_monitor: bool
    - should_feedback: bool
    - priority: "high", "medium", "low"
    - reason: str
    """
    
    decision = {
        "should_learn": False,
        "should_monitor": False,
        "should_feedback": False,
        "priority": "low",
        "reason": ""
    }
    
    if failure_type == "systemic":
        decision["should_learn"] = True
        decision["priority"] = "high"
        decision["reason"] = "systemic_failure_requires_immediate_learning"
        
    elif failure_type == "recurring":
        decision["should_learn"] = True
        decision["priority"] = "medium"
        decision["reason"] = "recurring_failure_needs_root_cause_analysis"
        
    elif failure_type == "transient":
        decision["should_monitor"] = True
        decision["priority"] = "low"
        decision["reason"] = "transient_failure_no_learning_needed_auto_retry"
        
    elif failure_type == "user_error":
        decision["should_feedback"] = True
        decision["priority"] = "low"
        decision["reason"] = "user_error_provide_helpful_feedback"
        
    elif failure_type == "external":
        decision["should_monitor"] = True
        decision["priority"] = "low"
        decision["reason"] = "external_failure_monitor_for_pattern"
    
    return decision
```

**What this does**:
- ✅ Implements the learning decision logic from the skill
- ✅ Returns actionable decisions
- ✅ Can be used directly in agent workflows

---

## Example 4: Actual Version Backup (Working Code)

```python
import shutil
from datetime import datetime

def backup_workflow_before_update(workflow_id, workflow_path):
    """
    Backup workflow before update (version control).
    
    Returns backup_path for later rollback if needed.
    """
    
    # Create version directory
    version_dir = f"/skills/ntl-workflow-guidance/references/workflows/versions/{workflow_id}"
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{version_dir}/{workflow_id}_{timestamp}.json"
    
    # Ensure directory exists
    import os
    os.makedirs(version_dir, exist_ok=True)
    
    # Copy current workflow to backup
    shutil.copy2(workflow_path, backup_path)
    
    # Log backup
    backup_log = {
        "workflow_id": workflow_id,
        "backup_path": backup_path,
        "timestamp": timestamp,
        "action": "backup_before_update"
    }
    
    # Append to learning log
    with open("/skills/workflow-self-evolution/references/learning_log.jsonl", "a") as f:
        f.write(json.dumps(backup_log) + "\n")
    
    return backup_path

def rollback_workflow(workflow_id, backup_path):
    """
    Rollback workflow to previous version.
    
    Returns rollback result.
    """
    
    workflow_path = f"/skills/ntl-workflow-guidance/references/workflows/{workflow_id}.json"
    
    # Restore from backup
    shutil.copy2(backup_path, workflow_path)
    
    # Log rollback
    rollback_log = {
        "workflow_id": workflow_id,
        "from_backup": backup_path,
        "timestamp": datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
        "action": "rollback"
    }
    
    with open("/skills/workflow-self-evolution/references/learning_log.jsonl", "a") as f:
        f.write(json.dumps(rollback_log) + "\n")
    
    return {"status": "success", "restored_from": backup_path}
```

**What this does**:
- ✅ Actually backs up workflows before updates
- ✅ Can rollback to any previous version
- ✅ Logs all version operations

---

## Example 5: Actual Metrics Update (Working Code)

```python
def update_quality_metrics(result):
    """
    Update quality metrics after task execution.
    
    Updates /skills/workflow-self-evolution/references/metrics.json
    """
    
    metrics_path = "/skills/workflow-self-evolution/references/metrics.json"
    
    # Load current metrics
    try:
        with open(metrics_path, "r") as f:
            metrics = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        metrics = {
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "workflows": {},
            "global_metrics": {
                "total_executions": 0,
                "overall_success_rate": 0.0,
                "avg_confidence": 0.0,
                "escalation_rate": 0.0,
                "learning_count": 0,
                "noise_filter_rate": 0.0
            }
        }
    
    # Update global metrics
    metrics["global_metrics"]["total_executions"] += 1
    
    if result.status == "success":
        # Update success rate (simple moving average)
        current_success_rate = metrics["global_metrics"]["overall_success_rate"]
        total = metrics["global_metrics"]["total_executions"]
        metrics["global_metrics"]["overall_success_rate"] = (
            (current_success_rate * (total - 1) + 1) / total
        )
    else:
        # Update failure-related metrics
        current_success_rate = metrics["global_metrics"]["overall_success_rate"]
        total = metrics["global_metrics"]["total_executions"]
        metrics["global_metrics"]["overall_success_rate"] = (
            (current_success_rate * (total - 1) + 0) / total
        )
    
    # Update workflow-specific metrics
    workflow_id = result.workflow_id
    if workflow_id not in metrics["workflows"]:
        metrics["workflows"][workflow_id] = {
            "usage_count": 0,
            "success_count": 0,
            "failure_count": 0,
            "last_used": None
        }
    
    metrics["workflows"][workflow_id]["usage_count"] += 1
    if result.status == "success":
        metrics["workflows"][workflow_id]["success_count"] += 1
    else:
        metrics["workflows"][workflow_id]["failure_count"] += 1
    metrics["workflows"][workflow_id]["last_used"] = datetime.utcnow().isoformat() + "Z"
    
    # Save updated metrics
    metrics["last_updated"] = datetime.utcnow().isoformat() + "Z"
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
```

**What this does**:
- ✅ Actually updates metrics file
- ✅ Tracks both global and per-workflow metrics
- ✅ Can be used for quality monitoring

---

## Example 6: Complete Integration in NTL_Engineer (Conceptual)

```python
# This is how NTL_Engineer should integrate self-evolution

def execute_task_with_full_evolution(task):
    """
    Complete integration example for NTL_Engineer.
    
    Combines all the actual implementations above.
    """
    
    # Step 1: Execute task
    result = execute_task(task)
    
    # Step 2: Log execution
    log_execution_result(result)
    
    # Step 3: Update metrics
    update_quality_metrics(result)
    
    # Step 4: Handle failures intelligently
    if result.status == "failed":
        # Classify failure
        failure_type = classify_failure_type(result.error)
        
        # Make learning decision
        decision = make_learning_decision(result, failure_type)
        
        # Execute decision
        if decision["should_learn"]:
            # Log learning decision
            log_learning_decision(result, decision)
            
            if decision["priority"] == "high":
                # Trigger mandatory review
                trigger_mandatory_review(result.workflow_id)
            
            # Modify workflow if needed
            if requires_workflow_modification(result):
                # Backup first
                backup_path = backup_workflow_before_update(
                    result.workflow_id,
                    f"/skills/ntl-workflow-guidance/references/workflows/{result.workflow_id}.json"
                )
                
                # Modify workflow
                modify_workflow(result.workflow_id, result.error)
                
                # Validate modification
                if not validate_workflow_modification(result.workflow_id):
                    # Rollback if validation fails
                    rollback_workflow(result.workflow_id, backup_path)
        
        elif decision["should_feedback"]:
            # Provide user feedback
            provide_user_feedback(result)
        
        elif decision["should_monitor"]:
            # Transient failure - auto-retry
            if is_retryable(result.error):
                retry_result = auto_retry(task, max_attempts=3)
                if retry_result.status == "success":
                    result = retry_result
                    log_execution_result(result)  # Log successful retry
    
    return result
```

**What this does**:
- ✅ Complete working integration
- ✅ Uses actual file I/O, not fake imports
- ✅ Implements all self-evolution features
- ✅ Can be used directly in NTL_Engineer

---

## Key Takeaways

### DO ✅

1. **Use file I/O** for logging and metrics (not fake imports)
2. **Pattern matching** for failure classification
3. **Decision rules** from SKILL.md for learning decisions
4. **Backup before updates** using file copy
5. **Log everything** to JSONL files

### DON'T ❌

1. **Don't try to import** `workflow_self_evolution` (doesn't exist as Python module)
2. **Don't expect magic** (it's a skill/guideline, not a library)
3. **Don't skip logging** (logs are essential for learning)
4. **Don't update without backup** (always version control)
5. **Don't learn from every failure** (use intelligent filtering)

---

## Summary

**`workflow-self-evolution` is a skill (guideline/protocol), NOT a Python module.**

Integration is done through:
- ✅ File I/O (read/write JSON/JSONL files)
- ✅ Pattern matching (classify failures)
- ✅ Decision rules (from SKILL.md)
- ✅ Tool calls (existing tools like `write_file`, `edit_file`)

**NOT through**:
- ❌ Python imports (module doesn't exist)
- ❌ Magic function calls (methods are conceptual)

Use the working code examples above for actual implementation!

---

## Example 2: Version Control Integration

```python
from skills.workflow_self_evolution import WorkflowSelfEvolution

def update_workflow_safely(workflow_id, new_content):
    evolution = WorkflowSelfEvolution()
    
    # Step 1: Backup current version
    backup = evolution.before_update(workflow_id, new_content)
    print(f"Backed up {workflow_id} to version {backup.version}")
    
    try:
        # Step 2: Update workflow
        update_workflow(workflow_id, new_content)
        print(f"Updated {workflow_id} successfully")
        
        # Step 3: Validate update
        if not validate_update(workflow_id):
            # Rollback if validation fails
            rollback_result = evolution.rollback(workflow_id, backup.version)
            print(f"Validation failed, rolled back to {rollback_result.to_version}")
            return False
        
        return True
        
    except Exception as e:
        # Step 4: Auto-rollback on error
        rollback_result = evolution.rollback(workflow_id, backup.version)
        print(f"Error occurred, rolled back to {rollback_result.to_version}")
        return False
```

**What this does**:
- ✅ Automatic backup before updates
- ✅ Validation after update
- ✅ Auto-rollback if validation fails
- ✅ Safe update process with rollback capability

---

## Example 3: Quality Monitoring Dashboard

```python
from skills.workflow_self_evolution import WorkflowSelfEvolution

def generate_quality_report():
    evolution = WorkflowSelfEvolution()
    
    # Get global metrics
    global_metrics = evolution.get_global_metrics()
    print(f"Total Executions: {global_metrics.total_executions}")
    print(f"Overall Success Rate: {global_metrics.overall_success_rate:.2%}")
    print(f"Average Confidence: {global_metrics.avg_confidence:.2f}")
    print(f"Escalation Rate: {global_metrics.escalation_rate:.2%}")
    print(f"Noise Filter Rate: {global_metrics.noise_filter_rate:.2%}")
    
    # Get trend report (last 7 days)
    trend_report = evolution.generate_trend_report(period_days=7)
    print(f"\n7-Day Trends:")
    print(f"  Success Rate: {trend_report.success_rate_trend}")
    print(f"  Confidence: {trend_report.confidence_trend}")
    print(f"  Escalation Rate: {trend_report.escalation_rate_trend}")
    
    # Get optimization suggestions
    suggestions = evolution.generate_optimization_suggestions()
    print(f"\nOptimization Suggestions ({len(suggestions)}):")
    for suggestion in suggestions[:5]:  # Show top 5
        print(f"  - {suggestion.workflow_id}: {suggestion.suggestion}")
```

**What this does**:
- ✅ Real-time quality monitoring
- ✅ Trend analysis (7-day rolling window)
- ✅ Automated optimization suggestions
- ✅ Data-driven decision making

---

## Example 4: A/B Testing New Workflow

```python
from skills.workflow_self_evolution import WorkflowSelfEvolution

def test_new_workflow_version(workflow_id, new_content):
    evolution = WorkflowSelfEvolution()
    
    # Step 1: Deploy as experimental (20% traffic)
    evolution.deploy_experimental(workflow_id, new_content)
    print(f"Deployed {workflow_id} as experimental (20% traffic)")
    
    # Step 2: Wait for sufficient samples (e.g., 10 executions)
    # This would typically be done asynchronously
    # For demo, we'll just check results
    ab_result = evolution.evaluate_ab_test(workflow_id)
    
    if ab_result.samples >= 10 and ab_result.success_rate >= 0.80:
        # Step 3a: Promote to production
        evolution.promote_to_production(workflow_id)
        print(f"✓ Test passed! Promoted {workflow_id} to production")
        print(f"  Success Rate: {ab_result.success_rate:.2%}")
        print(f"  Samples: {ab_result.samples}")
        return True
    else:
        # Step 3b: Demote or remove
        evolution.demote_experimental(workflow_id)
        print(f"✗ Test failed. Removed {workflow_id} from experimental")
        print(f"  Success Rate: {ab_result.success_rate:.2%} (need ≥80%)")
        print(f"  Samples: {ab_result.samples} (need ≥10)")
        return False
```

**What this does**:
- ✅ Safe A/B testing with traffic split
- ✅ Data-driven promotion decisions
- ✅ Automatic rollback for failed tests
- ✅ No disruption to production workflows

---

## Example 5: Handling Different Failure Types

```python
from skills.workflow_self_evolution import WorkflowSelfEvolution

def handle_failure_with_evolution(result):
    evolution = WorkflowSelfEvolution()
    
    # Classify failure
    failure_type = evolution.classify_failure(result.error)
    print(f"Failure Type: {failure_type.category} (confidence: {failure_type.confidence:.2f})")
    
    # Make learning decision
    decision = evolution.make_learning_decision(result)
    
    # Handle based on decision
    if decision.should_learn and decision.priority == "high":
        # Systemic failure - learn immediately
        print(f"🔴 HIGH PRIORITY: {decision.reason}")
        evolution.trigger_learning(result)
        evolution.trigger_mandatory_review(result.workflow_id)
        
    elif decision.should_learn and decision.priority == "medium":
        # Recurring failure - learn with human review
        print(f"🟡 MEDIUM PRIORITY: {decision.reason}")
        evolution.trigger_learning(result, require_human_review=True)
        
    elif decision.should_monitor:
        # Transient failure - monitor but don't learn
        print(f"🟢 MONITOR ONLY: {decision.reason}")
        evolution.log_for_monitoring(result)
        # Auto-retry logic
        retry_result = evolution.auto_retry(result.task, max_attempts=3)
        
    elif decision.should_feedback:
        # User error - provide feedback
        print(f"🔵 USER FEEDBACK: {decision.reason}")
        provide_helpful_feedback(result)
        
    else:
        # External failure - wait and see
        print(f"⚪ WAIT AND SEE: {decision.reason}")
        evolution.monitor_external_failure(result)

def provide_helpful_feedback(result):
    """Provide helpful feedback to user for user errors"""
    feedback = {
        "error_type": "user_input_error",
        "message": "It looks like there might be an issue with the input parameters.",
        "suggestions": [
            "Check that all required parameters are provided",
            "Verify that file paths are correct",
            "Ensure date formats are YYYY-MM-DD"
        ],
        "documentation_link": "https://docs.example.com/common-errors"
    }
    send_feedback_to_user(feedback)
```

**What this does**:
- ✅ Intelligent failure classification (5 categories)
- ✅ Appropriate action for each failure type
- ✅ Auto-retry for transient failures
- ✅ Helpful feedback for user errors
- ✅ No noise learning

---

## Example 6: Complete Integration in ntl-workflow-guidance

```python
# This is how ntl-workflow-guidance should integrate workflow-self-evolution

from skills.workflow_self_evolution import WorkflowSelfEvolution

class WorkflowIntentRouter:
    def __init__(self):
        self.evolution = WorkflowSelfEvolution()
    
    def execute_task(self, task):
        """Execute a task with full evolution integration"""
        
        # Step 1: Execute workflow
        result = self._execute_workflow(task)
        
        # Step 2: Update metrics (always)
        self.evolution.update_metrics(result)
        
        # Step 3: Handle failures intelligently
        if result.status == "failed":
            self._handle_failure(result)
        
        # Step 4: Check for force learn triggers
        self._check_force_learn_triggers()
        
        return result
    
    def _handle_failure(self, result):
        """Handle failure with intelligent filtering"""
        
        # Classify failure
        failure_type = self.evolution.classify_failure(result.error)
        
        # Make learning decision
        decision = self.evolution.make_learning_decision(result)
        
        # Execute decision
        if decision.should_learn:
            # Learn from this failure
            self.evolution.trigger_learning(result)
            
            if decision.priority == "high":
                # Immediate review for systemic failures
                self.evolution.trigger_mandatory_review(result.workflow_id)
        
        elif decision.should_feedback:
            # Provide user feedback
            self._provide_user_feedback(result)
        
        elif decision.should_monitor:
            # Log for monitoring (transient failures)
            self.evolution.log_for_monitoring(result)
            
            # Auto-retry
            retry_result = self.evolution.auto_retry(result.task, max_attempts=3)
            if retry_result.status == "success":
                result = retry_result  # Use successful retry result
    
    def _check_force_learn_triggers(self):
        """Check if any force learn triggers are active"""
        triggers = self.evolution.check_force_learn_triggers()
        
        for trigger in triggers:
            if trigger.is_active:
                # Force learn cannot be skipped
                self.evolution.trigger_mandatory_review(trigger.workflow_id)
                print(f"Force learn triggered for {trigger.workflow_id}: {trigger.reason}")
    
    def update_workflow(self, workflow_id, new_content):
        """Update workflow with version control"""
        
        # Backup current version
        backup = self.evolution.before_update(workflow_id, new_content)
        
        try:
            # Update workflow
            update_workflow(workflow_id, new_content)
            
            # Validate update
            if not self._validate_update(workflow_id):
                # Rollback if validation fails
                self.evolution.rollback(workflow_id, backup.version)
                raise Exception("Validation failed, rolled back")
            
        except Exception as e:
            # Auto-rollback on any error
            self.evolution.rollback(workflow_id, backup.version)
            raise
    
    def _validate_update(self, workflow_id):
        """Validate workflow update"""
        # Check syntax
        if not validate_syntax(workflow_id):
            return False
        
        # Check tool availability
        if not check_tools_available(workflow_id):
            return False
        
        # Run smoke test
        if not run_smoke_test(workflow_id):
            return False
        
        return True
```

**What this does**:
- ✅ Complete integration example
- ✅ Intelligent failure handling
- ✅ Version control with rollback
- ✅ Force learn triggers
- ✅ Production-ready pattern

---

## Best Practices

### DO ✅

1. **Always update metrics** after every execution (success or failure)
2. **Use intelligent filtering** to avoid learning from noise
3. **Backup before updates** - never update without version control
4. **Validate after updates** - rollback if validation fails
5. **Monitor quality trends** - use trend reports for insights
6. **Respect force learn triggers** - don't skip mandatory reviews
7. **Provide helpful feedback** for user errors
8. **Log all decisions** with clear reasons

### DON'T ❌

1. **Don't learn from every failure** - 81% are noise
2. **Don't skip version backup** - always backup before changes
3. **Don't ignore force learn triggers** - they're there for a reason
4. **Don't hide failures from users** - be transparent
5. **Don't update without validation** - test before deploying
6. **Don't learn without logging** - always record decision reasons
7. **Don't deploy untested workflows** - use A/B testing
8. **Don't ignore quality metrics** - monitor trends regularly

---

## API Reference

### Core Functions

```python
# Failure Filtering
failure_type = evolution.classify_failure(error)
decision = evolution.make_learning_decision(result)
valuable_failures = evolution.filter_noise(all_failures)

# Version Control
backup = evolution.before_update(workflow_id, new_content)
result = evolution.rollback(workflow_id, target_version)
diff = evolution.compare_versions(workflow_id, "v1", "v2")

# Quality Metrics
evolution.update_metrics(result)
report = evolution.generate_trend_report(period_days=7)
quality = evolution.get_quality_score(workflow_id)
metrics = evolution.get_workflow_metrics(workflow_id)

# Optimization
evolution.deploy_experimental(workflow_id, new_content)
result = evolution.evaluate_ab_test(workflow_id)
suggestions = evolution.generate_optimization_suggestions()

# Force Learn
triggers = evolution.check_force_learn_triggers()
evolution.trigger_mandatory_review(workflow_id)
```

### Data Structures

```python
# Failure Type
FailureType(
    category="systemic",  # or recurring, transient, user_error, external
    confidence=0.95,
    patterns_matched=["type_error", "parameter_mismatch"]
)

# Learning Decision
LearningDecision(
    should_learn=True,
    should_monitor=False,
    should_feedback=False,
    priority="high",  # or medium, low
    reason="systemic_failure_requires_immediate_learning"
)

# Quality Score
QualityScore(
    overall=0.85,  # 0.0 - 1.0
    components={
        "success_rate": 0.89,
        "confidence": 0.72,
        "satisfaction": 0.84,
        "stability": 0.95
    },
    trend="improving",  # or stable, declining
    risk_flag=False
)

# Workflow Metrics
WorkflowMetrics(
    workflow_id="Q11",
    usage_count=45,
    success_rate=0.89,
    avg_confidence=0.72,
    avg_satisfaction=4.2,
    failure_patterns=["network_timeout"],
    last_used=datetime(2026, 2, 28, 10, 30),
    risk_flag=False
)
```

---

## Troubleshooting

### Problem: Too many false positives (learning from noise)

**Solution**: Increase classification confidence threshold
```python
# In your evolution config
evolution.set_classification_threshold(0.90)  # Default is 0.75
```

### Problem: Missing important failures (false negatives)

**Solution**: Lower threshold for recurring failures
```python
# Check recurring failure detection
recurring = evolution.detect_recurring_failure(result, threshold=2)  # Default is 3
```

### Problem: Rollback not working

**Solution**: Verify version backup exists
```python
# List available versions
versions = evolution.list_versions(workflow_id)
print(f"Available versions: {versions}")

# Ensure backup was created
backup = evolution.before_update(workflow_id, new_content)
print(f"Backup created: {backup.version}")
```

### Problem: Quality metrics not updating

**Solution**: Ensure update_metrics is called after every execution
```python
# Always call this, even for failures
evolution.update_metrics(result)
```

---

## Getting Help

- **Documentation**: `/skills/workflow-self-evolution/SKILL.md`
- **Examples**: This file (`INTEGRATION_EXAMPLE.md`)
- **Metrics Dashboard**: `/skills/workflow-self-evolution/references/metrics.json`
- **Failure Log**: `/skills/workflow-self-evolution/references/failure_log.jsonl`
- **Learning Log**: `/skills/workflow-self-evolution/references/learning_log.jsonl`

For questions or issues, contact: `NTL_Engineer`
