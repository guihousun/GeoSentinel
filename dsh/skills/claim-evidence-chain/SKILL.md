---
name: claim-evidence-chain
description: Use when a research answer states conclusions that must be traceable. Defines the Claim–Evidence artifact written by geo_write_evidence, its validation gates, and how contradictions and uncertainty are recorded.
---

# 断言—证据链

重要研究结论不要只写在散文里。给出实质判断的回答，用 `geo_write_evidence` 落一份**断言—证据矩阵**，让每条结论都能追溯到具体产物或来源。

## 什么时候写

- 多步骤研究、事件影响评估、跨来源比较，以及任何给出「发生了什么/为什么」判断的回答；
- 存在相互矛盾来源、覆盖缺口或无法交叉核实的信息时（更要写，而不是省略）。

简单事实问答不必写。

## 产物

`geo_write_evidence` 写入 `outputs/<作业ID>/`：`*.json`（机器可校验）与同名 `*.md`（人可读矩阵），并附 `provenance.json`（sha256 与来源清单）。

```json
{
  "topic": "研究问题",
  "method": "数据、方法、参数的一句话说明",
  "claims": [
    {
      "id": "C1",
      "text": "断言文本",
      "type": "factual | temporal | spatial | statistical | relational | interpretation",
      "confidence": "high | medium | low",
      "time": "2024-01..2024-06",
      "location": "若开邦",
      "evidence": [
        {
          "kind": "dataset | document | remote_sensing | statistic | web",
          "stance": "supporting | contradicting | neutral",
          "source": "outputs/<作业ID>/antl.csv",
          "retrievedAt": "2026-09-09T12:00:00Z",
          "note": "为什么它支持或反驳",
          "confidence": "high | medium | low"
        }
      ],
      "interpretation": "属于解释、不是观测事实的部分"
    }
  ],
  "limitations": ["覆盖缺口、替代解释、不确定性"]
}
```

## 校验规则（工具会检查）

1. 每条断言至少 1 条证据；`evidence[].source` 必须是**真实存在**的 `inputs/` 或 `outputs/` 文件，或 http(s) 链接。
2. 网页证据给出 `retrievedAt` 检索时间。
3. 只有 `contradicting`/`neutral` 证据、没有 `supporting` 证据的断言，`confidence` 用 `low`。
4. `limitations` 至少 1 条。
5. 观测与解释分开：`interpretation` 里的内容不写成事实。

## 使用建议

- 先写产物、再写报告：`geo_write_report` 的 `source_paths` 引用这份证据链。
- 不要为了让矩阵好看而删掉反驳证据；分歧保留并说明影响。
- 同一栅格的重复计算是计算交叉校验，不是独立证据，不要当作第二条 `supporting`。
- 数值结论引用平台工具产物；网页只作线索与来源。
