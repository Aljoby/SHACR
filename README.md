# Folder Contents

| File / Folder | Purpose |
| :--- | :--- |
| `data/smart_building_dataset.csv` | Labeled dataset of 203 rules across 70 apartments, used for evaluation. |
| `src/custom_tools_local.py` | MCP tool server (stdio transport) for Claude Desktop. Exposes `scan_apartment`, `explain_conflict`, `recommend_best_repair`, `validate_repair`, and `apply_approved_repair`. |
| `src/ingest_snippet.py` | Ingestion script. Loads all YAML files into Neo4j, creating Apartment, Room, Device, Capability, State, Rule, Trigger, Condition, Action nodes and APPLIES_IN, HAS_DEVICE, HAS_TRIGGER, HAS_CONDITION, HAS_ACTION, AFFECTS relationships. |
| `prompts/system_prompt.txt` | Full SHGRAG system prompt with all 7 few-shot examples (3 conflict + 4 clean-chain). Paste into Claude Project Instructions. Remove examples to reduce to 3-shot then to zero-shot; the core prompt framework remains identical. |
| `YAML files/` | 70 apartment definitions (`smart_building_*.yaml`) — rooms, devices, capabilities, states, rules, triggers, conditions, actions, AFFECTS relationships. Source data for graph ingestion. |
