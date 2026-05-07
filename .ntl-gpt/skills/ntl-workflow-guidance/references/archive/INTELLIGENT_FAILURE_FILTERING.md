# 智能失败过滤机制设计

*版本：v1.0 | 创建时间：2026-02-28 | 状态：设计评审*

---

## ⚠️ 问题陈述

### 原始设计的风险

**原始方案**: "每次失败都学习"

**问题**:
```
失败总数：100 次
├─ 偶发网络超时：40 次 (噪音) ❌
├─ 用户输入错误：25 次 (噪音) ❌
├─ GEE API 临时故障：20 次 (噪音) ❌
└─ 真正的工作流缺陷：15 次 (信号) ✅

如果全部学习 → 80% 噪音污染工作流库
```

**后果**:
- 工作流库被低质量更新污染
- 偶发问题被误认为系统性缺陷
- 频繁修改导致工作流不稳定
- 代理对失败学习失去信心

---

## ✅ 改进方案：智能分层过滤

### 核心原则

```
❌ 不是每次失败都学习
✅ 只学习有价值的系统性/重复性失败
✅ 先分类 → 再过滤 → 最后决策
```

---

## 📊 失败分类体系 (5 类)

### 1️⃣ Systemic (系统性错误) - 🔴 高优先级

**定义**: 工作流或工具设计缺陷导致的错误

**特征**:
- 每次执行相同工作流都会失败
- 错误信息指向代码逻辑问题
- 与网络、外部 API 无关

**典型错误**:
```python
"tool_parameter_error"      # 工具参数错误
"step_sequence_error"       # 步骤顺序错误
"missing_tool"              # 缺少必要工具
"type_error"                 # 类型错误
"attribute_error"           # 属性错误
"key_error"                 # 键错误
```

**处理策略**: 
- ✅ **立即学习**
- ✅ 触发强制审查
- ✅ 标记为高风险工作流

---

### 2️⃣ Recurring (重复性错误) - 🟡 中优先级

**定义**: 同一工作流重复发生的错误 (排除偶发类型)

**特征**:
- 7 天内发生 ≥3 次
- 错误类型相同
- 不是偶发/用户错误

**判断逻辑**:
```python
def is_recurring_failure(workflow_id, error_type, window_days=7, threshold=3):
    failures = get_failures(workflow_id, window_days)
    
    # 过滤掉偶发和用户错误
    countable = [
        f for f in failures
        if f.type not in ["transient", "user_error"]
    ]
    
    # 统计相同错误类型
    similar = [f for f in countable if f.error_type == error_type]
    
    return len(similar) >= threshold
```

**处理策略**:
- ✅ **触发学习**
- ✅ 分析根本原因
- ⚠️ 需要人工确认 (避免误判)

---

### 3️⃣ Transient (偶发性错误) - 🟢 低优先级

**定义**: 临时性、偶发的外部问题

**特征**:
- 重试可能成功
- 与网络、API 限流相关
- 不指向工作流缺陷

**典型错误**:
```python
"network_timeout"           # 网络超时
"connection_reset"          # 连接重置
"api_rate_limit"            # API 限流
"temporary_unavailable"     # 临时不可用
"server_busy"               # 服务器繁忙
```

**处理策略**:
- ❌ **不学习**
- ✅ 仅记录日志
- ✅ 自动重试 (最多 3 次)
- ✅ 监控趋势 (如果频繁发生则升级)

**噪音过滤效果**: 预计过滤 40% 的失败噪音

---

### 4️⃣ User Error (用户错误) - 🟢 低优先级

**定义**: 用户输入错误或使用不当

**特征**:
- 工作流本身正确
- 用户提供的参数/文件有问题
- 错误信息明确指向用户输入

**典型错误**:
```python
"file_not_found"            # 文件不存在
"invalid_boundary"          # 边界无效
"wrong_parameters"          # 参数错误
"permission_denied"         # 权限不足
"invalid_date_format"       # 日期格式错误
```

**处理策略**:
- ❌ **不学习** (不修改工作流)
- ✅ 给用户提供清晰反馈
- ✅ 提供修正建议
- ✅ 记录常见问题用于改进文档

**噪音过滤效果**: 预计过滤 25% 的失败噪音

---

### 5️⃣ External (外部依赖错误) - 🟡 中优先级

**定义**: 外部服务/API 故障

**特征**:
- 工作流正确
- 依赖的第三方服务故障
- 不在我们控制范围内

**典型错误**:
```python
"gee_api_error"             # GEE API 错误
"data_source_unavailable"   # 数据源不可用
"third_party_failure"       # 第三方服务故障
"authentication_failed"     # 认证失败 (非用户错误)
```

**处理策略**:
- ⚠️ **监控趋势**
- ❌ 单次失败不学习
- ✅ 重复发生 (≥3 次/7 天) → 触发学习
- ✅ 考虑添加容错机制

**噪音过滤效果**: 预计过滤 80% 的外部错误噪音

---

## 🎯 智能决策流程

### 决策树

```
失败发生
  ↓
[步骤 1] 分类失败类型
  ↓
  ├─ Systemic → ✅ 立即学习 + 强制审查
  ├─ Recurring → ✅ 触发学习 + 人工确认
  ├─ Transient → ❌ 仅记录 + 自动重试
  ├─ User Error → ❌ 仅反馈 + 不修改工作流
  └─ External → ⚠️ 监控趋势 + 重复才学习
  ↓
[步骤 2] 记录决策原因
  ↓
[步骤 3] 执行相应行动
```

### 伪代码实现

```python
def handle_failure(task_result):
    # 步骤 1: 分类
    failure_type = classify_failure(task_result)
    
    # 步骤 2: 决策
    decision = make_learning_decision(failure_type, task_result)
    
    # 步骤 3: 记录
    log_failure({
        "workflow_id": task_result.workflow_id,
        "failure_type": failure_type,
        "decision": decision,
        "timestamp": datetime.utcnow()
    })
    
    # 步骤 4: 执行
    if decision.should_learn:
        trigger_learning(task_result, priority=decision.priority)
    elif decision.should_monitor:
        add_to_monitoring(task_result)
    elif decision.should_feedback:
        provide_user_feedback(task_result)
    
    return decision

def classify_failure(task_result):
    error_message = str(task_result.error).lower()
    error_type = type(task_result.error).__name__
    
    # 偶发错误模式
    transient_patterns = [
        "timeout", "temporary", "rate limit", "connection reset",
        "network", "unavailable", "server busy"
    ]
    
    # 用户错误模式
    user_error_patterns = [
        "file not found", "invalid input", "missing parameter",
        "boundary not found", "permission denied", "invalid date"
    ]
    
    # 系统性错误模式
    systemic_patterns = [
        "type error", "attribute error", "key error",
        "missing tool", "invalid workflow", "step failed",
        "index error", "value error"
    ]
    
    # 外部错误模式
    external_patterns = [
        "gee api", "data source", "third party",
        "authentication", "service unavailable"
    ]
    
    # 分类逻辑 (优先级：systemic > transient > user > external)
    if any(p in error_message for p in systemic_patterns):
        return "systemic"
    elif any(p in error_message for p in transient_patterns):
        return "transient"
    elif any(p in error_message for p in user_error_patterns):
        return "user_error"
    elif any(p in error_message for p in external_patterns):
        return "external"
    else:
        return "recurring"  # 默认归类为重复性，需要进一步检查

def make_learning_decision(failure_type, task_result):
    """
    返回决策对象:
    {
        "should_learn": bool,
        "should_monitor": bool,
        "should_feedback": bool,
        "priority": "high|medium|low",
        "reason": str
    }
    """
    if failure_type == "systemic":
        return {
            "should_learn": True,
            "priority": "high",
            "reason": "systemic_failure_requires_immediate_learning"
        }
    
    if failure_type == "transient":
        return {
            "should_learn": False,
            "should_monitor": True,
            "priority": "low",
            "reason": "transient_failure_no_learning_needed"
        }
    
    if failure_type == "user_error":
        return {
            "should_learn": False,
            "should_feedback": True,
            "priority": "low",
            "reason": "user_error_provide_feedback_only"
        }
    
    if failure_type == "external":
        # 检查是否重复发生
        if is_recurring_failure(task_result, threshold=3):
            return {
                "should_learn": True,
                "priority": "medium",
                "reason": "recurring_external_failure"
            }
        else:
            return {
                "should_learn": False,
                "should_monitor": True,
                "priority": "low",
                "reason": "single_external_failure_monitor_only"
            }
    
    if failure_type == "recurring":
        return {
            "should_learn": True,
            "priority": "high",
            "reason": "recurring_failure_pattern_detected"
        }
    
    # 默认不学习
    return {
        "should_learn": False,
        "priority": "low",
        "reason": "uncategorized_default_no_learning"
    }
```

---

## 📈 预期效果

### 噪音过滤效果

```
总失败数：100 次
├─ Transient (偶发): 40 次 → 过滤 100% ✅
├─ User Error (用户): 25 次 → 过滤 100% ✅
├─ External (外部): 20 次 → 过滤 80% (16 次) ✅
└─ Systemic (系统): 15 次 → 全部学习 ✅

实际学习次数：19 次 (19%)
噪音过滤率：81%
```

### 学习质量提升

| 指标 | 优化前 | 优化后 | 改进 |
|------|-------|-------|------|
| 学习准确率 | ~20% | ~85% | **+325%** |
| 工作流稳定性 | 频繁变动 | 稳定更新 | **显著提升** |
| 代理信心 | 低 (怕学错) | 高 (精准学习) | **显著提升** |
| 用户满意度 | 低 (噪音多) | 高 (质量好) | **+50%** |

---

## 🔍 监控与验证

### 关键监控指标

```python
MONITORING_METRICS = {
    "failure_classification_accuracy": {
        "description": "失败分类准确率",
        "target": "> 85%",
        "measurement": "人工抽样验证"
    },
    "noise_filter_rate": {
        "description": "噪音过滤率",
        "target": "> 75%",
        "measurement": "过滤数 / 总失败数"
    },
    "learning_precision": {
        "description": "学习精准度",
        "target": "> 80%",
        "measurement": "有效学习数 / 总学习数"
    },
    "false_negative_rate": {
        "description": "漏学率 (应该学但没学)",
        "target": "< 10%",
        "measurement": "漏学数 / 应学总数"
    },
    "false_positive_rate": {
        "description": "误学率 (不该学但学了)",
        "target": "< 15%",
        "measurement": "误学数 / 总学习数"
    }
}
```

### 验证流程

```python
def validate_learning_quality():
    """
    每周验证学习质量
    """
    # 抽样检查
    samples = random_sample(failure_log, n=50)
    
    # 人工验证
    validation_results = []
    for sample in samples:
        human_review = manual_review(sample)
        validation_results.append({
            "ai_decision": sample.decision,
            "human_decision": human_review,
            "match": sample.decision == human_review
        })
    
    # 计算准确率
    accuracy = sum(r["match"] for r in validation_results) / len(validation_results)
    
    # 如果准确率 < 85%, 触发模型优化
    if accuracy < 0.85:
        trigger_model_optimization(validation_results)
    
    return {
        "accuracy": accuracy,
        "samples": len(samples),
        "needs_optimization": accuracy < 0.85
    }
```

---

## 📋 实施清单

### 阶段 1: 基础分类 (4 小时)

- [ ] 定义 5 类失败模式
- [ ] 实现分类器函数
- [ ] 创建失败日志结构
- [ ] 单元测试分类准确率

### 阶段 2: 决策逻辑 (3 小时)

- [ ] 实现学习决策函数
- [ ] 配置重复失败检测
- [ ] 实现自动重试机制
- [ ] 集成到失败处理流程

### 阶段 3: 监控验证 (2 小时)

- [ ] 创建监控指标
- [ ] 实现自动验证流程
- [ ] 设置告警阈值
- [ ] 文档化验证流程

### 阶段 4: 持续优化 (持续)

- [ ] 每周人工抽样验证
- [ ] 根据误判案例优化分类器
- [ ] 更新错误模式库
- [ ] 分享学习质量报告

---

## 🎯 总结

### 核心改进

| 维度 | 优化前 | 优化后 |
|------|-------|-------|
| **学习策略** | 全部学习 | 智能过滤 |
| **准确率** | ~20% | ~85% |
| **噪音过滤** | 0% | 81% |
| **工作流稳定性** | 频繁变动 | 稳定更新 |

### 关键原则

1. **质量优先于数量**: 宁可漏学，不可误学
2. **分类是核心**: 准确分类是智能过滤的前提
3. **重复是关键**: 单次失败不学习，重复才学习
4. **监控不可少**: 持续验证学习质量

### 预期价值

- ✅ **减少噪音**: 81% 的失败噪音被过滤
- ✅ **提升质量**: 学习准确率从 20% → 85%
- ✅ **增强信心**: 代理更愿意使用学习机制
- ✅ **用户满意**: 工作流质量稳定提升

---

*设计文档版本：v1.0*  
*创建时间：2026-02-28*  
*下次评审：2026-03-07*  
*负责人：NTL_Engineer*
