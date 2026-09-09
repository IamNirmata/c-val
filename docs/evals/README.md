# Evaluation Research Drafts

These scripts are retained research inputs, not the deployed evaluator. Do not
run them against production databases: they use historical keys, global
threshold fallbacks and unconstrained output writes, including table replacement.
Their pandas/NumPy dependencies are not part of the production runtime.

Current implementation: [evaluation_engine](../../evaluation_engine/).
Current schema: [evaluation-database-design.md](../components_design/evaluation-database-design.md).