# AuditFlow Agent Context

- 日期：2026-03-30
- 产品：SOC 2 证据治理、映射评审与审计包导出

## 已完成的设计层

- `PRD.md`
- `ARCHITECTURE.md`
- `DATABASE.md`
- `API.md`
- `WORKFLOW.md`
- `PROMPT_TOOL.md`
- `INTEGRATIONS.md`

## 当前实现结论

- 产品层代码位于 `src/auditflow_app/`
- `bootstrap.py` 只装配 AuditFlow 相关工作流，并基于 SQLAlchemy 运行时存储构建产品服务
- `service.py` 已覆盖工作区、周期、导入、映射、缺口、叙述、导出、检索、记忆记录与运行时能力
- `routes.py` 已提供真实 FastAPI 接口，包含会话鉴权、分页 envelope、领域错误映射与事件流
- `repository.py` 已持久化工作区、周期、控制状态、证据块、映射、缺口、导出、审计日志、记忆记录与幂等键
- 导入处理支持上传、Jira、Confluence 三类来源；上传支持多文本格式与若干二进制格式的启发式解析
- 检索层已是混合检索：词法索引 + 稠密向量 + ANN 风格候选裁剪
- 工具适配器已绑定产品仓储，可供工作流调用证据搜索、工件读取、控制项查询、历史读取与快照校验
- 产品模型网关支持本地与可选 OpenAI 响应/嵌入路径，并在运行时能力接口中暴露实际模式
- 回放能力已经覆盖固定导入到导出的场景，能保存 baseline 与生成 JSON/Markdown 报告

## 当前实现边界

- 二进制解析仍以启发式抽取为主，不是完整 OCR 或办公文档深度解析
- `pgvector`/持久化 ANN 仍属于可选模式，不是默认生产级索引方案
- 评审工作台覆盖了领取、分配、冲突与快照控制，但还没有更复杂的多人协同语义

## 建议续做方向

1. 把当前向量检索升级为稳定的 `pgvector` 或更强 ANN 基础设施
2. 扩展二进制解析与 OCR，减少对启发式抽取的依赖
3. 补强评审工作台的多人协作、重新分配、优先级与冲突恢复语义
4. 扩展 replay 基线版本化与更系统的回归场景库

## 本地事实来源

共享运行时的本地源仍是 `D:\project\SharedAgentCore`。
