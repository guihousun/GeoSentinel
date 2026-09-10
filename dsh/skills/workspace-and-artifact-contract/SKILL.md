---
name: workspace-and-artifact-contract
description: Use whenever reading inputs or writing outputs. Defines the per-chat workspace layout, path rules, artifact identity, and the no-fabricated-checksum rule.
---

# 工作区与产物契约

- 输入读 `inputs/`（**项目级**：本对话里直接写 `inputs/<文件名>`）；新产物写 `outputs/<作业ID>/`。`geo_*` 工具的相对路径以本对话工作区为基准。
- 每个研究对话有独立工作区；不能访问其他用户或其他项目的目录。`read`/`glob`/`grep`/`read_document` 只读，并被平台限制在本对话工作区、本项目 `inputs/` 与技能目录内。
- `write`/`edit` 可写，但**只能写本对话的 `outputs/`**：上传文件、项目资料 `inputs/`、`memory/` 与技能库都不可写。需要自己生成脚本、中间表或备注时用它们，落在 `outputs/` 里。
- 手写文件如果承载观测数值或统计结果，必须在文件内注明来源作业 ID（例如 `来源：作业 <jobId> / outputs/<file>`），结论里沿用同一来源；手写内容不算独立观测，也不能当成新证据。
- 技能自带的 `references/`、`scripts/` 位于技能目录，只读；按需读取，不要整目录通读。
- 用户上传的文件在**本对话**的 `.dsh-uploads/<对话ID>/` 下（`geo_list_files` 的 `uploads` 会列出）；项目资料在项目级 `inputs/`（`geo_list_files` 的 `inputs`）。两者都能读。
- 读用户上传的 PDF/DOCX/XLSX：用 `geo_list_files` 给出的相对路径（`.dsh-uploads/<对话ID>/<文件名>` 或项目 `inputsRoot` 拼出的文件名）交给 `read_document`。绝对路径只作为工具参数，不要写进报告或回答。
- 消息里给紧凑引用；大表、日志、二进制留在文件里。
- 不要自己计算、猜测、复制或占位填写 `sha256` 与字节数——产物身份由平台绑定。缺少校验工具不构成阻塞理由。
- 保留失败的脚本、日志与部分产物以便审计；**作业提交成功或退出码为 0 不等于产物已验证**。
- 产物必须能被 `geo_inspect_raster`、`geo_inspect_vector`、`geo_validate_geodata` 复核；写报告用 `geo_write_report`，并在 `source_paths` 里引用真实存在的输入或产物路径。
