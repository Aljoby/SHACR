# custom_tools.py
# ─────────────────────────────────────────────────────────────────────────────
# SHRAG Custom MCP Server
# Defines 5 tools. Each tool owns its own Cypher queries.
#
# Tools:
#   1. scan_apartment(apartment_id)
#   2. explain_conflict(conflict_id)
#   3. recommend_best_repair(conflict_id)
#   4. apply_approved_repair(repair_id, repair_object)
#   5. validate_repair(apartment_id, conflict_id)
#
# Run:  python custom_tools.py
# ─────────────────────────────────────────────────────────────────────────────

import os
import json
from dotenv import load_dotenv
from neo4j import GraphDatabase
from mcp.server.fastmcp import FastMCP

# Load .env file (local dev). No effect if running in a managed environment.
load_dotenv()

# ── Neo4j connection ──────────────────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE")

# Fail fast — crash immediately with a clear message if any var is missing
_missing = [k for k, v in {
    "NEO4J_URI":      NEO4J_URI,
    "NEO4J_USERNAME": NEO4J_USERNAME,
    "NEO4J_PASSWORD": NEO4J_PASSWORD,
    "NEO4J_DATABASE": NEO4J_DATABASE,
}.items() if not v]

if _missing:
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(_missing)}\n"
        "Add them to your .env file."
    )

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
)

def run_read(cypher: str, params: dict = {}) -> list:
    with driver.session(database=NEO4J_DATABASE) as session:
        return session.run(cypher, **params).data()

def run_write(cypher: str, params: dict = {}) -> list:
    with driver.session(database=NEO4J_DATABASE) as session:
        return session.run(cypher, **params).data()
        
# ── MCP server ────────────────────────────────────────────────────────────────
mcp = FastMCP("shrag-apartment-tools")

# ─────────────────────────────────────────────────────────────────────────────
# TOOL 1 — scan_apartment
# Returns dashboard metrics + detected conflicts with IDs.
# This is the entry point. All conflict IDs originate here.
# ─────────────────────────────────────────────────────────────────────────────
@mcp.tool()
def scan_apartment(apartment_id: str) -> dict:
    """
    Full apartment scan. Returns:
    - apartmentDashboard: room/device/rule counts for UI display
    - rawSubgraph: full cyber-physical rule structure for Claude to reason over
      and classify conflicts (logical, semantic, physical) without pre-labelling
    """

    # ── 1. Rules summary ─────────────────────────────────────────────────────
    rules_data = run_read("""
        MATCH (r:Rule)-[:APPLIES_IN]->(apt:Apartment {apartmentId: $apt_id})
        RETURN
            r.ruleId      AS ruleId,
            r.name        AS ruleName,
            r.description AS ruleDescription
        ORDER BY r.ruleId
    """, {"apt_id": apartment_id})

    # ── 2. Devices and states ─────────────────────────────────────────────────
    devices_data = run_read("""
        MATCH (apt:Apartment {apartmentId: $apt_id})-[:HAS_ROOM]->(room:Room)
              -[:HAS_DEVICE]->(dev:Device)
        OPTIONAL MATCH (dev)-[:HAS_STATE]->(s:State)
        RETURN
            dev.deviceId   AS deviceId,
            dev.name       AS deviceName,
            dev.type       AS deviceType,
            room.name      AS roomName,
            collect({
                stateId: s.stateId,
                name:    s.name,
                value:   s.value
            }) AS states
    """, {"apt_id": apartment_id})

    # ── 3. Contexts and EVars ─────────────────────────────────────────────────
    contexts_data = run_read("""
        MATCH (apt:Apartment {apartmentId: $apt_id})
        OPTIONAL MATCH (apt)-[:HAS_CONTEXT]->(ctx:Context)
        OPTIONAL MATCH (apt)-[:HAS_EVAR]->(ev:EVar)
        RETURN
            collect(DISTINCT {
                contextId: ctx.contextId,
                name:      ctx.name,
                value:     ctx.value
            }) AS contexts,
            collect(DISTINCT {
                evarId: ev.evarId,
                name:   ev.name,
                value:  ev.value
            }) AS evars
    """, {"apt_id": apartment_id})

    # ── 4a. Rules + actions + targets ────────────────────────────────────────
    rules_subgraph = run_read("""
        MATCH (r:Rule)-[:APPLIES_IN]->(apt:Apartment {apartmentId: $apt_id})
        OPTIONAL MATCH (r)-[:HAS_ACTION]->(a:Action)
        OPTIONAL MATCH (a)-[:TARGETS]->(at)
        OPTIONAL MATCH (a)-[:APPLIES_TO]->(dev:Device)
        RETURN
            r.ruleId        AS ruleId,
            r.name          AS ruleName,
            r.description   AS ruleDescription,
            r.platform      AS rulePlatform,
            collect(DISTINCT {
                action:     a.action,
                value:      a.value,
                targetId:   COALESCE(at.stateId, at.capId, at.evarId),
                targetName: at.name,
                targetType: labels(at)[0],
                deviceId:   dev.deviceId,
                deviceName: dev.name,
                deviceType: dev.type
            }) AS actions
    """, {"apt_id": apartment_id})

    # ── 4b. Triggers + conditions + capabilities + EVars ─────────────────────
    triggers_subgraph = run_read("""
        MATCH (r:Rule)-[:APPLIES_IN]->(apt:Apartment {apartmentId: $apt_id})
        OPTIONAL MATCH (r)-[:HAS_TRIGGER]->(t:Trigger)
        OPTIONAL MATCH (t)-[:TARGETS]->(tt)
        OPTIONAL MATCH (r)-[:HAS_CONDITION]->(c:Condition)
        OPTIONAL MATCH (c)-[:TARGETS]->(ct)
        OPTIONAL MATCH (r)-[:HAS_ACTION]->(a:Action)-[:APPLIES_TO]->(dev:Device)
        OPTIONAL MATCH (dev)-[:HAS_CAP]->(cap:Capability)
        OPTIONAL MATCH (cap)-[:AFFECTS]->(ev:EVar)
        RETURN
            r.ruleId AS ruleId,
            collect(DISTINCT {
                triggerType: t.type,
                value:       t.value,
                operator:    t.operator,
                targetId:    COALESCE(tt.stateId, tt.evarId, tt.contextId),
                targetName:  tt.name,
                targetType:  labels(tt)[0]
            }) AS triggers,
            collect(DISTINCT {
                conditionType: c.type,
                value:         c.value,
                operator:      c.operator,
                targetId:      COALESCE(ct.stateId, ct.evarId, ct.contextId),
                targetName:    ct.name,
                targetType:    labels(ct)[0]
            }) AS conditions,
            collect(DISTINCT {
                capabilityName: cap.name,
                affectsEVar:    ev.name,
                evarId:         ev.evarId
            }) AS capabilities
    """, {"apt_id": apartment_id})

    # ── Merge both subgraphs by ruleId ────────────────────────────────────────
    triggers_map = {row["ruleId"]: row for row in triggers_subgraph}
    raw_subgraph = []
    for row in rules_subgraph:
        rid = row["ruleId"]
        extra = triggers_map.get(rid, {})
        raw_subgraph.append({
            "ruleId":          rid,
            "ruleName":        row["ruleName"],
            "ruleDescription": row["ruleDescription"],
            "rulePlatform":    row["rulePlatform"],
            "actions":         row["actions"],
            "triggers":        extra.get("triggers", []),
            "conditions":      extra.get("conditions", []),
            "capabilities":    extra.get("capabilities", []),
        })

    # ── 5. Room count ─────────────────────────────────────────────────────────
    room_count_data = run_read("""
        MATCH (apt:Apartment {apartmentId: $apt_id})-[:HAS_ROOM]->(room:Room)
        RETURN count(room) AS roomCount
    """, {"apt_id": apartment_id})

    # ── Assemble return ───────────────────────────────────────────────────────────
    room_count   = room_count_data[0]["roomCount"] if room_count_data else 0
    device_count = len(devices_data)
    rule_count   = len(rules_data)

    return {
        "apartmentDashboard": {
            "apartmentId":   apartment_id,
            "roomCount":     room_count,
            "deviceCount":   device_count,
            "ruleCount":     rule_count,
            "conflictCount": 0,        # Claude determines this from rawSubgraph
            "rules":         rules_data,
            "devices":       devices_data,
            "contexts":      contexts_data[0].get("contexts", []) if contexts_data else [],
            "evars":         contexts_data[0].get("evars", [])    if contexts_data else [],
        },
        "rawSubgraph": raw_subgraph    # Claude reasons over this to detect all conflict types
    }


# ─────────────────────────────────────────────────────────────────────────────
# TOOL 2 — explain_conflict
# Deep-dives into both rules involved in a conflict.
# Returns plain language explanation for dashboard display.
# ─────────────────────────────────────────────────────────────────────────────
@mcp.tool()
def explain_conflict(conflict_id: str) -> dict:
    """
    Given a conflict_id (format: ruleAId_ruleBId), retrieves full details
    of both rules — triggers, conditions, actions, targets — and returns
    a structured explanation of what the conflict is and what it causes.
    """

    # conflict_id format: "ruleA_ruleB"
    parts = conflict_id.split("_", 1)
    if len(parts) != 2:
        return {"error": f"Invalid conflict_id format: {conflict_id}. Expected ruleAId_ruleBId"}

    rule_a_id, rule_b_id = parts[0], parts[1]

    # ── Full rule details query (your original Aura query, applied to both rules)
    def get_rule_details(rule_id: str) -> dict:
        result = run_read("""
            MATCH (r:Rule {ruleId: $ruleId})
            OPTIONAL MATCH (r)-[:HAS_TRIGGER]->(t:Trigger)
            OPTIONAL MATCH (t)-[:TARGETS]->(tt)
            OPTIONAL MATCH (r)-[:HAS_CONDITION]->(c:Condition)
            OPTIONAL MATCH (c)-[:TARGETS]->(ct)
            OPTIONAL MATCH (r)-[:HAS_ACTION]->(a:Action)
            OPTIONAL MATCH (a)-[:TARGETS]->(at)
            OPTIONAL MATCH (a)-[:APPLIES_TO]->(d:Device)
            RETURN
                r.ruleId      AS ruleId,
                r.name        AS ruleName,
                r.description AS ruleDescription,
                r.platform    AS rulePlatform,
                COLLECT(DISTINCT {
                    triggerId: t.triggerId,
                    type:      t.type,
                    value:     t.value,
                    operator:  t.operator,
                    target: {
                        id:   COALESCE(tt.evarId, tt.stateId),
                        name: tt.name,
                        type: LABELS(tt)[0]
                    }
                }) AS triggers,
                COLLECT(DISTINCT {
                    conditionId: c.conditionId,
                    type:        c.type,
                    value:       c.value,
                    operator:    c.operator,
                    target: {
                        id:   COALESCE(ct.contextId, ct.stateId),
                        name: ct.name,
                        type: LABELS(ct)[0]
                    }
                }) AS conditions,
                COLLECT(DISTINCT {
                    actionId: a.actionId,
                    type:     a.type,
                    action:   a.action,
                    target: {
                        id:   COALESCE(at.capId, at.stateId),
                        name: at.name,
                        type: LABELS(at)[0]
                    },
                    device: CASE WHEN d IS NOT NULL
                        THEN {id: d.deviceId, name: d.name, type: d.type}
                        ELSE NULL
                    END
                }) AS actions
        """, {"ruleId": rule_id})
        return result[0] if result else {}

    rule_a = get_rule_details(rule_a_id)
    rule_b = get_rule_details(rule_b_id)

    if not rule_a or not rule_b:
        return {"error": f"Could not find rules for conflict {conflict_id}"}

    return {
        "conflictId": conflict_id,
        "conflictName": f"{rule_a.get('ruleName', rule_a_id)} vs {rule_b.get('ruleName', rule_b_id)}",
        "ruleA": rule_a,
        "ruleB": rule_b,
        "explanation": {
            "summary": (
                f"Two rules are targeting the same device with contradicting actions. "
                f"'{rule_a.get('ruleName')}' and '{rule_b.get('ruleName')}' "
                f"cannot both be active simultaneously without causing a conflict."
            ),
            "ruleADescription": rule_a.get("ruleDescription", ""),
            "ruleBDescription": rule_b.get("ruleDescription", ""),
            "triggerConflict": (
                f"Rule A triggers on: {[t.get('type') for t in rule_a.get('triggers', [])]} | "
                f"Rule B triggers on: {[t.get('type') for t in rule_b.get('triggers', [])]}"
            ),
            "actionConflict": (
                f"Rule A actions: {[a.get('action') for a in rule_a.get('actions', [])]} | "
                f"Rule B actions: {[a.get('action') for a in rule_b.get('actions', [])]}"
            )
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
# TOOL 3 — recommend_best_repair
# Analyzes a conflict and returns a structured repair object.
# Read-only. No writes happen here.
# ─────────────────────────────────────────────────────────────────────────────
@mcp.tool()
def recommend_best_repair(conflict_id: str) -> dict:
    """
    Given a conflict_id, retrieves full rule details and device states,
    reasons over the conflict, and returns a bestFitSolution description
    plus a structured repairObject ready for apply_approved_repair.
    """

    parts = conflict_id.split("_", 1)
    if len(parts) != 2:
        return {"error": f"Invalid conflict_id format: {conflict_id}"}

    rule_a_id, rule_b_id = parts[0], parts[1]

    # ── Full rule details for both rules ──────────────────────────────────────
    rules_data = run_read("""
        MATCH (r:Rule)
        WHERE r.ruleId IN [$ruleAId, $ruleBId]
        OPTIONAL MATCH (r)-[:HAS_ACTION]->(a:Action)-[:APPLIES_TO]->(d:Device)
        OPTIONAL MATCH (r)-[:HAS_CONDITION]->(c:Condition)
        OPTIONAL MATCH (r)-[:HAS_TRIGGER]->(t:Trigger)
        RETURN
            r.ruleId      AS ruleId,
            r.name        AS ruleName,
            r.description AS ruleDescription,
            r.platform    AS rulePlatform,
            collect(DISTINCT {
                actionId: a.actionId,
                action:   a.action,
                type:     a.type,
                deviceId: d.deviceId,
                device:   d.name
            }) AS actions,
            collect(DISTINCT {
                conditionId: c.conditionId,
                type:        c.type,
                value:       c.value,
                operator:    c.operator
            }) AS conditions,
            collect(DISTINCT {
                triggerId: t.triggerId,
                type:      t.type,
                value:     t.value
            }) AS triggers
    """, {"ruleAId": rule_a_id, "ruleBId": rule_b_id})

    # ── Device current states for the affected device ─────────────────────────
    device_states = run_read("""
        MATCH (r:Rule {ruleId: $ruleAId})-[:HAS_ACTION]->(a:Action)-[:APPLIES_TO]->(d:Device)
        OPTIONAL MATCH (d)-[:HAS_STATE]->(s:State)
        RETURN
            d.deviceId AS deviceId,
            d.name     AS deviceName,
            d.type     AS deviceType,
            collect({
                stateId: s.stateId,
                name:    s.name,
                value:   s.value
            }) AS currentStates
    """, {"ruleAId": rule_a_id})

    rule_a = next((r for r in rules_data if r["ruleId"] == rule_a_id), {})
    rule_b = next((r for r in rules_data if r["ruleId"] == rule_b_id), {})
    device = device_states[0] if device_states else {}

    # ── Allowed repair action space ───────────────────────────────────────────────
    # Claude selects from these operations only.
    # No destructive graph changes. No deletion of physical nodes.
    ALLOWED_REPAIRS = [
        {
            "operation": "modify_trigger",
            "description": "Modify a rule's trigger — change its type, value, or operator.",
            "when_to_use": "When the conflict is caused by both rules firing on the same trigger event."
        },
        {
            "operation": "add_condition",
            "description": "Add a condition to a rule so it only fires when the other rule is inactive.",
            "when_to_use": "When both rules are valid but must not fire simultaneously. Default safe choice."
        },
        {
            "operation": "remove_condition",
            "description": "Remove an overly restrictive condition that is causing the conflict.",
            "when_to_use": "When a condition is incorrectly blocking a rule that should be allowed to fire."
        },
        {
            "operation": "refine_condition",
            "description": "Tighten or adjust an existing condition's value or operator.",
            "when_to_use": "When a condition is too broad and causes unintended overlap with another rule."
        },
        {
            "operation": "modify_action",
            "description": "Change a rule's action target, value, or type to avoid contradiction.",
            "when_to_use": "When two rules send contradicting commands to the same device."
        },
        {
            "operation": "add_priority",
            "description": "Add a priority property to both rules so the higher priority one wins.",
            "when_to_use": "When both rules are valid but one should always take precedence."
        }
    ]
    
    # ── Forbidden operations — never suggest these ────────────────────────────────
    # delete_rule, delete_device, delete_room, delete_apartment,
    # remove physical relationships (HAS_DEVICE, HAS_ROOM, HAS_STATE, HAS_CAP),
    # invent new devices, rooms, states, or rules.
    
    # ── Build repair recommendation ───────────────────────────────────────────────
    # Select the best fitting operation from ALLOWED_REPAIRS based on conflict data.
    # Priority: add_condition is the safest default.
    # Escalate to modify_action if actions directly contradict.
    # Use add_priority if both rules are equally valid.
    
    action_a = rule_a.get("actions", [{}])[0].get("action", "") if rule_a.get("actions") else ""
    action_b = rule_b.get("actions", [{}])[0].get("action", "") if rule_b.get("actions") else ""
    
    # Determine best operation
    if action_a and action_b and action_a != action_b:
        # Direct action contradiction — modify the action of the lower priority rule
        selected_operation = "modify_action"
        repair_object = {
            "operation":  "modify_action",
            "targetRule": rule_b_id,
            "parameters": {
                "actionId":   rule_b.get("actions", [{}])[0].get("actionId", ""),
                "newAction":  action_a,
                "reason":     f"Align with dominant rule '{rule_a.get('ruleName', rule_a_id)}'"
            }
        }
        selected_description = next(
            r["description"] for r in ALLOWED_REPAIRS if r["operation"] == "modify_action"
        )
    else:
        # Default safe repair — add mutual exclusion condition
        selected_operation = "add_condition"
        repair_object = {
            "operation":  "add_condition",
            "targetRule": rule_b_id,
            "parameters": {
                "conditionType":  "rule_inactive",
                "referencedRule": rule_a_id,
                "operator":       "equals",
                "value":          "inactive"
            }
        }
        selected_description = next(
            r["description"] for r in ALLOWED_REPAIRS if r["operation"] == "add_condition"
        )
    
    # Always append metadata update
    metadata_update = {
        "operation":  "update_metadata",
        "targetRule": rule_b_id,
        "parameters": {
            "conflictStatus": "detected",
            "repairStatus":   "recommended",
            "explanation":    f"Conflict with rule '{rule_a.get('ruleName', rule_a_id)}'",
            "validationResult": "pending"
        }
    }
    
    return {
        "conflictId":        conflict_id,
        "deviceId":          device.get("deviceId", ""),
        "deviceName":        device.get("deviceName", ""),
        "currentStates":     device.get("currentStates", []),
        "ruleA":             rule_a,
        "ruleB":             rule_b,
        "allowedRepairs":    ALLOWED_REPAIRS,
        "selectedOperation": selected_operation,
        "bestFitSolution": (
            f"[{selected_operation}] {selected_description} "
            f"Targeting rule '{rule_b.get('ruleName', rule_b_id)}'."
        ),
        "repairObject":      repair_object,
        "metadataUpdate":    metadata_update,
        "forbidden": [
            "delete_rule", "delete_device", "delete_room", "delete_apartment",
            "remove HAS_DEVICE", "remove HAS_ROOM", "remove HAS_STATE",
            "invent new nodes"
        ]
}
# ─────────────────────────────────────────────────────────────────────────────
# TOOL 4 — apply_approved_repair
# Executes an approved repair against the database.
# The ONLY write tool. Should only be called after user confirmation.
# ─────────────────────────────────────────────────────────────────────────────
@mcp.tool()
def apply_approved_repair(repair_id: str, repair_object: dict) -> dict:
    """
    Executes an approved repair against Neo4j.
    repair_id: a reference ID for logging (e.g. the conflict_id)
    repair_object: the exact object returned by recommend_best_repair.

    Supported operations:
    - add_condition      : adds a mutual exclusion condition to a rule
    - modify_action      : changes a rule's action to remove contradiction
    - modify_trigger     : changes a rule's trigger type, value, or operator
    - refine_condition   : tightens an existing condition's value or operator
    - remove_condition   : removes an overly restrictive condition
    - add_priority       : assigns priority values to resolve precedence

    Forbidden: delete_rule, delete_device, delete_room, delete_apartment,
    removing physical relationships, inventing new nodes.
    """

    operation = repair_object.get("operation")
    params    = repair_object.get("parameters", {})
    target    = repair_object.get("targetRule")

    if not operation or not target:
        return {
            "success": False,
            "repairId": repair_id,
            "error": "Invalid repair_object: missing operation or targetRule"
        }

    try:
        if operation == "add_condition":
            run_write("""
                MATCH (r:Rule {ruleId: $targetRule})
                CREATE (c:Condition {
                    conditionId:    $targetRule + '_excl_' + $referencedRule,
                    type:           $conditionType,
                    referencedRule: $referencedRule,
                    operator:       $operator,
                    value:          $value
                })
                CREATE (r)-[:HAS_CONDITION]->(c)
                RETURN c.conditionId AS createdConditionId
            """, {
                "targetRule":     target,
                "conditionType":  params.get("conditionType", "rule_inactive"),
                "referencedRule": params.get("referencedRule", ""),
                "operator":       params.get("operator", "equals"),
                "value":          params.get("value", "inactive")
            })
            message = (
                f"Condition added to rule '{target}': "
                f"activates only when '{params.get('referencedRule')}' is inactive."
            )

        elif operation == "disable_rule":
            run_write("""
                MATCH (r:Rule {ruleId: $targetRule})
                SET r.active = false
                RETURN r.ruleId AS disabledRule
            """, {"targetRule": target})
            message = f"Rule '{target}' has been disabled."

        else:
            return {
                "success":  False,
                "repairId": repair_id,
                "error":    f"Unknown operation: {operation}"
            }

        return {
            "success":   True,
            "repairId":  repair_id,
            "operation": operation,
            "target":    target,
            "message":   message
        }

    except Exception as e:
        return {
            "success":  False,
            "repairId": repair_id,
            "error":    str(e)
        }


# ─────────────────────────────────────────────────────────────────────────────
# TOOL 5 — validate_repair
# Re-checks a specific conflict after a repair has been applied.
# Confirms whether the conflict is resolved or still present.
# ─────────────────────────────────────────────────────────────────────────────
@mcp.tool()
def validate_repair(apartment_id: str, conflict_id: str) -> dict:
    """
    After a repair is applied, re-checks whether the specific conflict
    still exists in the graph. Returns resolved status and current rule states.
    """

    parts = conflict_id.split("_", 1)
    if len(parts) != 2:
        return {"error": f"Invalid conflict_id format: {conflict_id}"}

    rule_a_id, rule_b_id = parts[0], parts[1]

    # ── Re-run conflict check for just these two rules ────────────────────────
    still_conflicting = run_read("""
        MATCH (r:Rule)-[:HAS_ACTION]->(a:Action)-[:APPLIES_TO]->(dev:Device)
        WHERE r.ruleId IN [$ruleAId, $ruleBId]
        WITH dev, collect(DISTINCT r) AS rules, collect(DISTINCT a) AS actions
        WHERE size(rules) > 1

        UNWIND range(0, size(actions)-2) AS i
        UNWIND range(i+1, size(actions)-1) AS j
        WITH actions[i] AS aA, actions[j] AS aB
        WHERE aA.action IS NOT NULL
        AND aB.action IS NOT NULL
        AND aA.action <> aB.action
        RETURN count(*) AS conflictCount
    """, {"ruleAId": rule_a_id, "ruleBId": rule_b_id})

    conflict_count = still_conflicting[0]["conflictCount"] if still_conflicting else 0
    is_resolved    = conflict_count == 0

    # ── Current state of both rules ───────────────────────────────────────────
    rule_states = run_read("""
        MATCH (r:Rule)
        WHERE r.ruleId IN [$ruleAId, $ruleBId]
        OPTIONAL MATCH (r)-[:HAS_CONDITION]->(c:Condition)
        RETURN
            r.ruleId  AS ruleId,
            r.name    AS ruleName,
            r.active  AS isActive,
            collect({
                conditionId: c.conditionId,
                type:        c.type,
                value:       c.value
            }) AS conditions
    """, {"ruleAId": rule_a_id, "ruleBId": rule_b_id})

    return {
        "conflictId":  conflict_id,
        "apartmentId": apartment_id,
        "isResolved":  is_resolved,
        "message": (
            "Conflict successfully resolved. Both rules can now coexist."
            if is_resolved else
            "Conflict still detected. The repair may not have applied correctly."
        ),
        "ruleStates":    rule_states,
        "conflictCount": conflict_count
    }


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Starting SHRAG MCP server...")
    mcp.run()
