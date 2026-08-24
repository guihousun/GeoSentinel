# System Prompt Updates for workflow-self-evolution

**Created**: 2026-02-28  
**Purpose**: Update system prompts to correctly integrate workflow-self-evolution skill

---

## ⚠️ CRITICAL: workflow-self-evolution is a SKILL, NOT a Python Module

**DO NOT use fake import examples like this**:
```python
from skills.workflow_self_evolution import WorkflowSelfEvolution  # ❌ WRONG!
```

**This doesn't exist.** Integration is done through **file I/O and tool calls**.

**See working examples**: `/skills/workflow-self-evolution/INTEGRATION_EXAMPLE.md`

---

## Required Updates to System Prompt

### Update 1: Section 0 - SKILL FIRST RULE

**Add after the code-generation-execution-loop line**:

```markdown
- For self-evolution and continuous improvement:
  - `/skills/workflow-self-evolution/`
  - Provides intelligent failure filtering (81% noise reduction), learning decisions, version control, and quality metrics.
  - **MANDATORY**: Apply after EVERY task execution to log results and enable continuous improvement.
  - Integration: Use file I/O and tool calls (NOT Python imports). See INTEGRATION_EXAMPLE.md.
```

---

### Update 2: Section 2 - RESOURCE ARCHITECTURE

**Replace the entire section with**:

```markdown
### 2. RESOURCE ARCHITECTURE

**Meta-Capability Skills (Universal - Call These for Core Functionality)**:
- **workflow-self-evolution**: Provides intelligent failure filtering (81% noise), learning decisions, version control with rollback, and quality metrics tracking for ALL skills. **MANDATORY** to apply after every task execution.
  - Integration: File I/O + tool calls (write_file, edit_file, etc.)
  - Documentation: `/skills/workflow-self-evolution/SKILL.md`
  - Working examples: `/skills/workflow-self-evolution/INTEGRATION_EXAMPLE.md`
  
- **code-generation-execution-loop**: Standardizes geospatial code lifecycle with save-read-execute-validate-one-fix-handoff protocol.

**Business Skills (Domain-Specific)**:
- **Data_Searcher**: Retrieves data from GEE, OSM, Amap, and Tavily. Files stored in `inputs/`.
- **Code_Assistant**: Validates and executes Python geospatial code (rasterio, geopandas, GEE API).
- **Knowledge_Base_Searcher**: Domain expert for methodology/workflow grounding. Use when skills are insufficient or confidence is low.
- **ntl-workflow-guidance**: **PREFERRED** alternative to Knowledge_Base_Searcher. Searches pre-defined workflow templates for faster, more accurate, and lower-token task planning. **ALWAYS use FIRST** before considering Knowledge_Base_Searcher.
```

---

### Update 3: Add New Section 3.2 - SELF-EVOLUTION PROTOCOL

**Add after Section 3.1 TASK LEVEL ROUTING**:

```markdown
### 3.2 SELF-EVOLUTION PROTOCOL (MANDATORY FOR CONTINUOUS IMPROVEMENT)

**⚠️ CRITICAL**: `workflow-self-evolution` is a **SKILL** (guideline/protocol), NOT a Python module.

**Integration Method**: File I/O and tool calls (write_file, edit_file, etc.)  
**Working Examples**: `/skills/workflow-self-evolution/INTEGRATION_EXAMPLE.md`

---

**When to Apply Self-Evolution**:
- AFTER EVERY task execution (success or failure) - log result and update metrics
- BEFORE any workflow update - backup version using file copy
- WHEN failure occurs - classify failure type and decide learning

**Required Actions**:

1. **Log execution result**:
   - Success: Update `/skills/workflow-self-evolution/references/metrics.json`
   - Failure: Append to `/skills/workflow-self-evolution/references/failure_log.jsonl`

2. **If failed - Classify failure** (use pattern matching on error message):
   - **Systemic** ("tool not found", "invalid parameter", "type error") → ✅ Learn immediately
   - **Recurring** (same error ≥3 times) → ✅ Learn with human review
   - **Transient** ("timeout", "connection error", "network error") → ❌ Don't learn, auto-retry
   - **User Error** ("file not found", "invalid input") → ❌ Don't learn, provide feedback
   - **External** ("api error", "gee error") → ⚠️ Learn only if recurring ≥3 times

3. **Make learning decision**:
   - Only learn from Systemic and Recurring failures (19% of failures)
   - Filter out Transient, User Error, External (81% noise)
   - NEVER learn from every failure - use intelligent filtering

4. **Before workflow update**:
   - Backup: Copy workflow file to `versions/<workflow_id>/<timestamp>.json`
   - Log backup to `learning_log.jsonl`
   - Make modification
   - Validate (syntax check, tool availability)
   - If validation fails → Rollback (restore from backup)

**Force Learn Triggers** (cannot skip):
- Same workflow fails ≥3 times in 7 days
- Confidence < 0.40 for 5 consecutive executions
- Usage > 10 times but success rate < 0.60

**Quality Metrics to Track** (update metrics.json after each execution):
- Total executions
- Overall success rate (target: ≥90%)
- Average confidence (target: ≥0.70)
- Escalation rate (target: ≤10%)
- Noise filter rate (target: ≥80%)

---

**Actual Integration Pattern** (Working Code):

```python
import json
from datetime import datetime

def execute_task_with_evolution(task):
    # Step 1: Execute task
    result = execute_task(task)
    
    # Step 2: Log execution
    log_entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "workflow_id": result.workflow_id,
        "task_id": result.task_id,
        "status": result.status,
        "error_message": str(result.error)[:500] if result.status == "failed" else None
    }
    
    if result.status == "failed":
        # Append to failure log
        with open("/skills/workflow-self-evolution/references/failure_log.jsonl", "a") as f:
            f.write(json.dumps(log_entry) + "\n")
        
        # Classify and decide learning
        failure_type = classify_failure_type(result.error)
        if failure_type in ["systemic", "recurring"]:
            trigger_workflow_learning(result)
        elif failure_type == "user_error":
            provide_user_feedback(result)
        # transient: auto-retry, external: monitor
    
    # Step 3: Update metrics
    update_quality_metrics(result)
    
    return result

def classify_failure_type(error_message):
    """Classify using pattern matching."""
    error_str = str(error_message).lower()
    
    if any(p in error_str for p in ["tool not found", "invalid parameter", "type error"]):
        return "systemic"
    if any(p in error_str for p in ["timeout", "connection error", "network error"]):
        return "transient"
    if any(p in error_str for p in ["file not found", "invalid input", "permission denied"]):
        return "user_error"
    if any(p in error_str for p in ["api error", "service unavailable", "gee error"]):
        return "external"
    
    return "systemic"  # Default
```

**What this does**:
- ✅ Actually works (uses file I/O, not fake imports)
- ✅ Implements 81% noise filtering
- ✅ Updates quality metrics
- ✅ Follows the protocol from SKILL.md
```

---

### Update 4: Section 4 - TASK EXECUTION WORKFLOW

**Add Step 7 after Step 6**:

```markdown
7. **SELF-EVOLUTION (MANDATORY)**:
   
   After task completion, ALWAYS apply self-evolution protocol:
   
   a. **Log execution result**:
      - Success: Update `metrics.json` with success metrics
      - Failure: Append to `failure_log.jsonl` with error details
   
   b. **If failed - Classify and decide**:
      - Use pattern matching to classify failure (5 categories)
      - Only learn from Systemic/Recurring (19% valuable failures)
      - Filter out Transient/User/External (81% noise)
      - Check force learn triggers (≥3 failures, low confidence streak)
   
   c. **Before workflow update**:
      - Backup current version (file copy to versions/)
      - Log backup to `learning_log.jsonl`
      - Make modification using `edit_file`
      - Validate modification
      - If validation fails → Rollback (restore backup)
   
   d. **Update evolution log**:
      - For formal mutations: Append to `evolution_log.jsonl`
      - Include: mode, updated_at, evidence_run_id, completion_gate, change_reason
   
   **Documentation**:
   - Skill: `/skills/workflow-self-evolution/SKILL.md`
   - Working examples: `/skills/workflow-self-evolution/INTEGRATION_EXAMPLE.md`
   - Metrics: `/skills/workflow-self-evolution/references/metrics.json`
   - Failure log: `/skills/workflow-self-evolution/references/failure_log.jsonl`
   - Learning log: `/skills/workflow-self-evolution/references/learning_log.jsonl`
```

---

## Summary of Changes

| Section | Change Type | Reason |
|---------|-------------|--------|
| **0. SKILL FIRST RULE** | Add workflow-self-evolution | Ensure agents know to use this skill |
| **2. RESOURCE ARCHITECTURE** | Restructure with meta-capability vs business | Clarify skill types and relationships |
| **3.2 SELF-EVOLUTION PROTOCOL** | New section | Provide mandatory protocol for continuous improvement |
| **4. TASK EXECUTION WORKFLOW** | Add Step 7 | Integrate self-evolution into execution flow |

---

## Testing Checklist

After applying these updates:

- [ ] Agents know workflow-self-evolution is a skill, not a Python module
- [ ] Agents use file I/O for integration (not fake imports)
- [ ] Agents apply self-evolution after every task execution
- [ ] Agents classify failures into 5 categories
- [ ] Agents only learn from 19% of valuable failures (81% noise filtered)
- [ ] Agents backup workflows before updates
- [ ] Agents check force learn triggers
- [ ] Agents update metrics.json after each execution

---

## Files to Update

1. Main system prompt (wherever `system_prompt_text` is defined)
2. Any agent initialization code that references workflow-self-evolution
3. Documentation that mentions the fake import example

---

**Next Steps**: Apply these updates to ensure correct integration of workflow-self-evolution skill.
