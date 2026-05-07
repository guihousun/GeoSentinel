# Workflow Intent Router 自更新机制审计

## 🔍 当前机制设计

### 1. 学习阶段分离

```markdown
Run Phase (执行阶段):
- 只读模式，不修改工作流
- 仅记录执行结果

Learn Phase (学习阶段):
- 仅在任务完成后触发
- 需要完成门控验证
```

### 2. 完成门控 (Completion Gate)

```json
{
  "execution.status": "success",
  "artifact_audit.pass": true
}
```

**要求**: 两个条件必须同时满足才能进行正式写回

### 3. 角色分离

| 角色 | 权限 | 限制 |
|------|------|------|
| Code_Assistant | 提案-only | 不可直接编辑文件 |
| NTL_Engineer | 决策 + 写入 | 唯一有权编辑正式工作流 |

### 4. 候选者机制

```
成功任务 → 直接写入 evolution_log.jsonl + 更新工作流
失败任务 → 写入 evolution_candidates.jsonl (待处理)
后续成功 → 可将候选者提升为正式更新
```

### 5. 安全边界

**允许**:
- 小步骤顺序优化
- 参数默认值优化
- 描述澄清
- 真正新任务类型的新增工作流

**禁止**:
- 未知工具插入
- 目标改变的改写
- 删除关键输出定义

---

## ⚠️ 发现的问题

### 问题 1: 学习信号单一 (严重度: 🔴 高)

**现状**: 只有成功任务才能触发学习
```
成功 → 学习 ✓
失败 → 不学习 ✗
```

**问题**:
- 失败经验无法转化为改进
- 同样的错误可能重复发生
- 丢失宝贵的负反馈信号

**实例**:
从 `evolution_candidates.jsonl` 看到:
```json
{
  "ts_utc": "2026-02-27T19:08:04Z",
  "status": "success",
  "artifact_pass": true,
  "learning_state": "candidate_only",
  "reason": "agent_decided_skip_evolution"
}
```
即使任务成功，agent 也可能跳过进化 → **学习机会流失**

---

### 问题 2: 被动学习机制 (严重度: 🟡 中)

**现状**: 依赖 agent 主动决定是否更新
```
agent_decided_skip_evolution → 无更新
```

**问题**:
- 没有强制学习触发器
- agent 可能因为惰性/时间压力跳过学习
- 学习质量依赖 agent 主观判断

**缺失**:
- ❌ 自动检测"可学习时刻"
- ❌ 强制学习触发条件
- ❌ 学习质量评估

---

### 问题 3: 无量化追踪指标 (严重度: 🔴 高)

**现状**: 只有定性记录，没有定量指标

**缺失指标**:
| 指标 | 重要性 | 当前状态 |
|------|--------|---------|
| 工作流使用频率 | 🔴 高 | ❌ 无追踪 |
| 各工作流成功率 | 🔴 高 | ❌ 无追踪 |
| 平均置信度变化 | 🔴 高 | ❌ 无追踪 |
| 升级率趋势 | 🔴 高 | ❌ 无追踪 |
| 重复错误检测 | 🟡 中 | ❌ 无追踪 |
| 用户满意度 | 🟡 中 | ❌ 无追踪 |
| 工作流半衰期 | 🟢 低 | ❌ 无追踪 |

**后果**: 无法回答:
- 哪个工作流最常用？
- 哪个工作流失败率最高？
- 置信度是否在提升？
- 升级率是否在下降？

---

### 问题 4: 无负反馈循环 (严重度: 🔴 高)

**现状**: 失败任务仅记录到 candidates，无后续行动

```
失败 → 记录到 candidates → 等待手动处理 → 可能永远不处理
```

**问题**:
- 同样的错误模式可能重复发生
- 没有自动标记"高风险工作流"
- 没有自动降級低质量工作流

**缺失机制**:
- ❌ 失败模式聚类分析
- ❌ 自动标记问题工作流
- ❌ 强制审查触发器
- ❌ 工作流质量评分系统

---

### 问题 5: 候选者积压风险 (严重度: 🟡 中)

**现状**: 
```
evolution_candidates.jsonl 只增不减
```

**风险**:
- 候选者积累但从不处理
- 没有自动清理机制
- 没有优先级排序

**建议**:
- 设置候选者数量上限 (如 50 条)
- 超过上限时自动触发批量处理
- 定期清理过期候选者 (>30 天)

---

### 问题 6: 无版本控制与回滚 (严重度: 🟠 中高)

**现状**: 直接编辑 JSON 文件，无版本历史

**风险**:
- 错误更新可能污染工作流库
- 无法回滚到之前的可用版本
- 无法比较不同版本的差异

**缺失**:
- ❌ 版本历史追踪
- ❌ 差异比较工具
- ❌ 一键回滚机制
- ❌ 更新前备份

---

### 问题 7: 无 A/B 测试机制 (严重度: 🟢 低)

**现状**: 工作流更新直接生效

**问题**:
- 无法验证新工作流是否真的更好
- 可能用更差的方案替换了好的方案
- 缺乏实证支持

**建议**:
- 新工作流先标记为 `experimental`
- 收集 10+ 次执行数据后再决定正式化
- 对比新旧工作流的成功率/置信度

---

### 问题 8: 无用户反馈集成 (严重度: 🟡 中)

**现状**: 仅依赖执行成功/失败信号

**缺失**:
- ❌ 用户满意度评分
- ❌ 用户明确反馈 (点赞/点踩)
- ❌ 用户建议收集
- ❌ 用户行为分析 (是否修改工作流)

**价值**: 用户反馈是最直接的质量信号

---

## ✅ 优点总结

尽管有上述问题，当前机制也有显著优点:

| 优点 | 价值 |
|------|------|
| **完成门控** | 防止低质量更新污染工作流库 |
| **角色分离** | Code_Assistant 不能直接修改，降低风险 |
| **候选者机制** | 失败经验至少被记录，未完全丢失 |
| **安全边界** | 明确允许/禁止的变更类型 |
| **演进日志** | 有完整的变更历史记录 |

---

## 🎯 改进建议

### 优先级 1 (必须实现) 🔴

#### 1.1 增加失败学习机制

```python
# 伪代码示例
def after_task_execution(task_result):
    if task_result.status == "failed":
        # 记录失败模式
        failure_pattern = analyze_failure(task_result)
        
        # 检查是否是重复失败
        if is_recurring_failure(failure_pattern):
            # 自动标记问题工作流
            mark_workflow_as_risky(task_result.workflow_id)
            
            # 触发强制审查
            trigger_mandatory_review(task_result.workflow_id)
        
        # 记录到失败日志
        log_failure(failure_pattern)
```

**关键改进**:
- 失败也触发学习
- 检测重复失败模式
- 自动标记高风险工作流

#### 1.2 增加量化追踪指标

创建 `metrics.json`:
```json
{
  "last_updated": "2026-02-28T12:00:00Z",
  "workflows": {
    "Q11": {
      "usage_count": 45,
      "success_rate": 0.89,
      "avg_confidence": 0.72,
      "last_used": "2026-02-28T10:30:00Z",
      "failure_patterns": ["boundary_not_found", "tool_timeout"]
    },
    "Q20": {
      "usage_count": 12,
      "success_rate": 0.67,
      "avg_confidence": 0.58,
      "last_used": "2026-02-27T15:20:00Z",
      "failure_patterns": ["gee_api_error"],
      "risk_flag": true
    }
  },
  "global_metrics": {
    "total_executions": 523,
    "overall_success_rate": 0.85,
    "avg_confidence_trend": [0.65, 0.68, 0.72],
    "escalation_rate_trend": [0.25, 0.18, 0.12]
  }
}
```

#### 1.3 强制学习触发器

```python
# 强制学习条件
FORCE_LEARN_TRIGGERS = {
    "recurring_failure": "同一工作流失败 3 次",
    "low_confidence_streak": "连续 5 次置信度 < 0.40",
    "high_usage_low_success": "使用>10 次但成功率 < 0.60",
    "user_complaint": "用户明确反馈不满意"
}

def check_force_learn_triggers():
    for trigger in FORCE_LEARN_TRIGGERS:
        if trigger.is_active():
            trigger_mandatory_review(trigger.workflow_id)
```

---

### 优先级 2 (强烈推荐) 🟡

#### 2.1 候选者自动处理

```python
# 候选者管理策略
CANDIDATE_MANAGEMENT = {
    "max_count": 50,
    "auto_process_threshold": 50,
    "expiry_days": 30,
    "priority_scoring": {
        "recurring_failure": 10,
        "high_confidence": 5,
        "recent": 3,
        "user_requested": 8
    }
}

def manage_candidates():
    candidates = load_candidates()
    
    # 超过上限，自动批量处理
    if len(candidates) > max_count:
        batch_process_top_candidates(candidates, top_n=10)
    
    # 清理过期候选者
    cleanup_expired_candidates(days=30)
```

#### 2.2 版本控制与回滚

```python
# 版本管理
def before_workflow_update(workflow_id, new_content):
    # 备份当前版本
    backup = {
        "workflow_id": workflow_id,
        "version": get_current_version(workflow_id),
        "content": load_workflow(workflow_id),
        "timestamp": datetime.utcnow()
    }
    
    # 保存到版本历史
    save_to_version_history(backup)
    
    # 应用更新
    update_workflow(workflow_id, new_content)
    
    # 记录回滚点
    log_rollback_point(workflow_id, backup["version"])

def rollback_workflow(workflow_id, target_version):
    # 回滚到指定版本
    version_data = get_version_history(workflow_id, target_version)
    update_workflow(workflow_id, version_data.content)
    log_rollback_action(workflow_id, target_version)
```

#### 2.3 用户反馈集成

```python
# 用户反馈收集
def collect_user_feedback(task_result):
    feedback = {
        "task_id": task_result.task_id,
        "workflow_id": task_result.workflow_id,
        "satisfaction": get_user_satisfaction(),  # 1-5 分
        "suggestions": get_user_suggestions(),
        "modified_workflow": check_if_user_modified_workflow(),
        "timestamp": datetime.utcnow()
    }
    
    # 更新工作流评分
    update_workflow_score(feedback)
    
    # 低分触发审查
    if feedback.satisfaction <= 2:
        trigger_review(feedback.workflow_id)
```

---

### 优先级 3 (锦上添花) 🟢

#### 3.1 A/B 测试机制

```python
# 实验性工作流
def deploy_new_workflow(workflow_id, new_content):
    # 标记为实验性
    workflow = {
        "id": workflow_id,
        "content": new_content,
        "status": "experimental",
        "ab_test": {
            "start_date": datetime.utcnow(),
            "traffic_split": 0.2,  # 20% 流量
            "success_threshold": 0.80,
            "min_samples": 10
        }
    }
    
    # 部署实验
    deploy_as_experimental(workflow)

def evaluate_ab_test(workflow_id):
    results = get_ab_test_results(workflow_id)
    
    if results.success_rate >= 0.80 and results.samples >= 10:
        # 晋升为正式工作流
        promote_to_production(workflow_id)
    else:
        # 降级或移除
        demote_or_remove(workflow_id)
```

#### 3.2 自动化工作流优化建议

```python
# 基于数据分析的优化建议
def generate_optimization_suggestions():
    suggestions = []
    
    # 分析低成功率工作流
    for workflow in get_low_success_workflows():
        suggestion = {
            "workflow_id": workflow.id,
            "issue": f"成功率仅 {workflow.success_rate:.2f}",
            "suggestion": "检查工具参数或考虑拆分工作流",
            "priority": "high"
        }
        suggestions.append(suggestion)
    
    # 分析置信度趋势
    for workflow in get_declining_confidence_workflows():
        suggestion = {
            "workflow_id": workflow.id,
            "issue": f"置信度连续下降 {workflow.decline_count} 次",
            "suggestion": "重新评估匹配关键词或调整阈值",
            "priority": "medium"
        }
        suggestions.append(suggestion)
    
    return suggestions
```

---

## 📋 实施路线图

### 阶段 1: 基础建设 (2026 Q2)

- [ ] 实现失败学习机制
- [ ] 创建量化指标追踪系统
- [ ] 实现强制学习触发器
- [ ] 建立候选者自动处理流程

**预期效果**: 
- 失败经验可转化为改进
- 可量化追踪工作流质量
- 自动检测并处理问题工作流

### 阶段 2: 质量保障 (2026 Q3)

- [ ] 实现版本控制与回滚
- [ ] 集成用户反馈系统
- [ ] 建立工作流质量评分
- [ ] 实现自动优化建议

**预期效果**:
- 可回滚错误更新
- 用户反馈驱动改进
- 主动发现并修复问题

### 阶段 3: 智能优化 (2026 Q4)

- [ ] 实现 A/B 测试机制
- [ ] 引入机器学习优化匹配
- [ ] 自动化工作流生成
- [ ] 建立工作流生态系统

**预期效果**:
- 数据驱动的工作流优化
- 更智能的意图匹配
- 自我进化的工作流库

---

## 🎯 总结评估

### 当前机制评分

| 维度 | 评分 | 说明 |
|------|------|------|
| **设计理念** | ⭐⭐⭐⭐ | 完成门控、角色分离等设计合理 |
| **安全性** | ⭐⭐⭐⭐ | 有安全边界和审批机制 |
| **完整性** | ⭐⭐ | 缺少失败学习、量化指标等关键机制 |
| **自动化程度** | ⭐⭐ | 过度依赖 agent 主观决策 |
| **可追溯性** | ⭐⭐⭐ | 有演进日志但无版本控制 |
| **用户参与** | ⭐ | 完全缺失用户反馈 |
| **持续改进** | ⭐⭐ | 被动学习，缺乏主动优化 |

**综合评分**: ⭐⭐⭐ (3/5) - **基本可用，但有重大改进空间**

### 核心问题

**当前机制无法保证"越做越熟练，犯错更少"，因为**:

1. ❌ 失败经验无法有效学习
2. ❌ 没有量化指标追踪进步
3. ❌ 重复错误无法自动检测
4. ❌ 低质量工作流无法自动降级
5. ❌ 用户反馈未纳入学习循环

### 关键改进方向

1. **从"成功学习"到"全量学习"**: 失败也是宝贵的学习信号
2. **从"定性记录"到"定量追踪"**: 用数据驱动优化
3. **从"被动学习"到"主动优化"**: 自动检测问题并触发改进
4. **从"单向更新"到"闭环反馈"**: 集成用户反馈形成闭环
5. **从"经验驱动"到"数据驱动"**: A/B 测试验证改进效果

---

## ✅ 立即行动建议

### 本周内完成 (高优先级)

1. **创建 metrics.json 追踪系统**
   - 记录每个工作流的使用次数、成功率、平均置信度
   - 每周生成趋势报告

2. **实现失败模式分析**
   - 聚类分析失败原因
   - 标记重复失败的工作流

3. **建立候选者处理流程**
   - 设置候选者上限 (50 条)
   - 每周自动批量处理 Top 10 候选者

### 本月内完成 (中优先级)

4. **实现版本控制**
   - 每次更新前自动备份
   - 支持一键回滚

5. **集成用户反馈**
   - 任务完成后询问满意度
   - 低分自动触发审查

### 本季度内完成 (低优先级)

6. **实验 A/B 测试**
   - 新工作流先标记为 experimental
   - 收集足够数据后再正式化

---

*审计时间：2026-02-28*  
*审计者：NTL_Engineer*  
*下次审计：2026-03-28 (月度审查)*
