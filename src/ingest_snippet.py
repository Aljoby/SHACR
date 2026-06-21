import os
from pathlib import Path
from typing import Dict, Any, List, Optional

from dotenv import load_dotenv
import yaml
from neo4j import GraphDatabase


# ---------------------------
# Env loading
# ---------------------------
BASE_DIR = Path(__file__).parent
ENV_PATH = BASE_DIR / "k.env"

if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)
else:
    load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DB = os.getenv("NEO4J_DB", "neo4j")

if not (NEO4J_URI and NEO4J_USER and NEO4J_PASSWORD):
    raise ValueError("Missing Neo4j environment variables: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD")

_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


# ---------------------------
# Constraints
# ---------------------------
CONSTRAINTS: List[str] = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Building) REQUIRE n.buildingId IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Apartment) REQUIRE n.apartmentId IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Room) REQUIRE n.roomId IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Device) REQUIRE n.deviceId IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Capability) REQUIRE n.capId IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:State) REQUIRE n.stateId IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:EVar) REQUIRE n.evarId IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Context) REQUIRE n.contextId IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Rule) REQUIRE n.ruleId IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Trigger) REQUIRE n.triggerId IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Condition) REQUIRE n.conditionId IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Action) REQUIRE n.actionId IS UNIQUE",
]


# ---------------------------
# YAML loading
# ---------------------------
def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"YAML not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"YAML root must be a mapping: {path}")
    return data


# ---------------------------
# Neo4j helpers
# ---------------------------
def ensure_constraints(tx):
    for c in CONSTRAINTS:
        tx.run(c)


def _merge_targets(session, from_label: str, from_key: str, from_id: str, targets: List[Dict[str, Any]]):
    """
    (Trigger|Condition|Action)-[:TARGETS {role}]->(Context|State|Capability|EVar)
    Fully bound label+id on both ends.
    """
    for tgt in targets or []:
        kind = tgt["kind"]
        target_id = tgt["id"]
        role = tgt.get("role")

        if kind == "Context":
            session.run(
                f"""
                MATCH (src:{from_label} {{{from_key}: $fromId}})
                MATCH (dst:Context {{contextId: $targetId}})
                MERGE (src)-[r:TARGETS]->(dst)
                SET r.role = $role
                """,
                fromId=from_id, targetId=target_id, role=role,
            )

        elif kind == "State":
            session.run(
                f"""
                MATCH (src:{from_label} {{{from_key}: $fromId}})
                MATCH (dst:State {{stateId: $targetId}})
                MERGE (src)-[r:TARGETS]->(dst)
                SET r.role = $role
                """,
                fromId=from_id, targetId=target_id, role=role,
            )

        elif kind == "Capability":
            session.run(
                f"""
                MATCH (src:{from_label} {{{from_key}: $fromId}})
                MATCH (dst:Capability {{capId: $targetId}})
                MERGE (src)-[r:TARGETS]->(dst)
                SET r.role = $role
                """,
                fromId=from_id, targetId=target_id, role=role,
            )

        elif kind == "EVar":
            session.run(
                f"""
                MATCH (src:{from_label} {{{from_key}: $fromId}})
                MATCH (dst:EVar {{evarId: $targetId}})
                MERGE (src)-[r:TARGETS]->(dst)
                SET r.role = $role
                """,
                fromId=from_id, targetId=target_id, role=role,
            )

        else:
            raise ValueError(f"Unsupported TARGET kind: {kind}")


def _merge_rule_scope_edge(session, rule_id: str, applies_in: Optional[Dict[str, Any]]):
    """
    (Rule)-[:APPLIES_IN]->(Apartment|Room)
    Expects:
      applies_in:
        kind: "Apartment" | "Room"
        id: "A1" | "R1"
    """
    if not applies_in:
        return

    kind = applies_in.get("kind")
    scope_id = applies_in.get("id")

    if kind == "Apartment":
        session.run(
            """
            MATCH (r:Rule {ruleId:$rid})
            MATCH (a:Apartment {apartmentId:$sid})
            MERGE (r)-[:APPLIES_IN]->(a)
            """,
            rid=rule_id, sid=scope_id
        )

    elif kind == "Room":
        session.run(
            """
            MATCH (r:Rule {ruleId:$rid})
            MATCH (rm:Room {roomId:$sid})
            MERGE (r)-[:APPLIES_IN]->(rm)
            """,
            rid=rule_id, sid=scope_id
        )

    else:
        raise ValueError(f"applies_in.kind must be Apartment or Room, got: {kind}")


# ---------------------------
# Ingestion
# ---------------------------
def ingest_from_yaml(yaml_filename: str):
    data = load_yaml(BASE_DIR / yaml_filename)

    topology = data["topology"]
    building = topology["building"]

    contexts = data.get("contexts", [])
    scoped_states = data.get("states", [])
    evars = data.get("evars", [])
    affects = data.get("semantics", {}).get("affects", [])
    rules = data.get("rules", [])

    with _driver.session(database=NEO4J_DB) as session:
        session.execute_write(ensure_constraints)

        # Building
        session.run(
            """
            MERGE (b:Building {buildingId:$id})
            SET b.name = $name
            """,
            id=building["id"],
            name=building.get("name", building["id"])
        )

        # Apartments -> Rooms -> Devices (+ CAP/STATE)
        for apt in building.get("apartments", []):
            session.run(
                """
                MATCH (b:Building {buildingId:$bid})
                MERGE (a:Apartment {apartmentId:$aid})
                SET a.name = $name
                MERGE (b)-[:HAS_APARTMENT]->(a)
                """,
                bid=building["id"],
                aid=apt["id"],
                name=apt.get("name", apt["id"])
            )

            for room in apt.get("rooms", []):
                session.run(
                    """
                    MATCH (a:Apartment {apartmentId:$aid})
                    MERGE (r:Room {roomId:$rid})
                    SET r.name = $name
                    MERGE (a)-[:HAS_ROOM]->(r)
                    """,
                    aid=apt["id"],
                    rid=room["id"],
                    name=room.get("name", room["id"])
                )

                for dev in room.get("devices", []):
                    session.run(
                        """
                        MATCH (r:Room {roomId:$rid})
                        MERGE (d:Device {deviceId:$did})
                        SET d.name = $name,
                            d.type = $type,
                            d.platform = $platform
                        MERGE (r)-[:HAS_DEVICE]->(d)
                        """,
                        rid=room["id"],
                        did=dev["id"],
                        name=dev.get("name", dev["id"]),
                        type=dev.get("type", "Unknown"),
                        platform=dev.get("platform", "Unknown")
                    )

                    for cap in dev.get("capabilities", []) or []:
                        session.run(
                            """
                            MERGE (c:Capability {capId:$cid})
                            SET c.name = $name
                            """,
                            cid=cap["id"],
                            name=cap.get("name", cap["id"])
                        )
                        session.run(
                            """
                            MATCH (d:Device {deviceId:$did})
                            MATCH (c:Capability {capId:$cid})
                            MERGE (d)-[:HAS_CAP]->(c)
                            """,
                            did=dev["id"], cid=cap["id"]
                        )

                    for st in dev.get("states", []) or []:
                        session.run(
                            """
                            MERGE (s:State {stateId:$sid})
                            SET s.name = $name
                            """,
                            sid=st["id"],
                            name=st.get("name", st["id"])
                        )
                        session.run(
                            """
                            MATCH (d:Device {deviceId:$did})
                            MATCH (s:State {stateId:$sid})
                            MERGE (d)-[:HAS_STATE]->(s)
                            """,
                            did=dev["id"], sid=st["id"]
                        )

        # EVars
        for ev in evars:
            session.run(
                """
                MERGE (e:EVar {evarId:$id})
                SET e.name = $name,
                    e.scope = $scope,
                    e.scopeId = $scopeId
                """,
                id=ev["id"],
                name=ev.get("name", ev["id"]),
                scope=ev.get("scope", "unknown"),
                scopeId=ev.get("scope_id")
            )

            if ev.get("scope") == "room" and ev.get("scope_id"):
                session.run(
                    """
                    MATCH (r:Room {roomId:$rid})
                    MATCH (e:EVar {evarId:$eid})
                    MERGE (r)-[:HAS_EVAR]->(e)
                    """,
                    rid=ev["scope_id"], eid=ev["id"]
                )

        # Contexts
        for ctx in contexts:
            session.run(
                """
                MERGE (c:Context {contextId:$id})
                SET c.name = $name,
                    c.scope = $scope,
                    c.scopeId = $scopeId
                """,
                id=ctx["id"],
                name=ctx.get("name", ctx["id"]),
                scope=ctx.get("scope", "unknown"),
                scopeId=ctx.get("scope_id")
            )

        # States
        for st in scoped_states:
            session.run(
                """
                MERGE (s:State {stateId:$id})
                SET s.name = $name,
                    s.scope = $scope,
                    s.scopeId = $scopeId
                """,
                id=st["id"],
                name=st.get("name", st["id"]),
                scope=st.get("scope", "unknown"),
                scopeId=st.get("scope_id")
            )

        # AFFECTS links
        for af in affects:
            cap_id = af["from_capability_id"]
            to_state_id = af.get("to_state_id")
            to_evar_id = af.get("to_evar_id")
            note = af.get("note")

            if to_state_id:
                session.run(
                    """
                    MATCH (c:Capability {capId:$cap})
                    MATCH (s:State {stateId:$sid})
                    MERGE (c)-[r:AFFECTS]->(s)
                    SET r.note = $note
                    """,
                    cap=cap_id, sid=to_state_id, note=note
                )

            if to_evar_id:
                session.run(
                    """
                    MATCH (c:Capability {capId:$cap})
                    MATCH (e:EVar {evarId:$eid})
                    MERGE (c)-[r:AFFECTS]->(e)
                    SET r.note = $note
                    """,
                    cap=cap_id, eid=to_evar_id, note=note
                )

        # Rules + T/C/A
        for rule in rules:
            rid = rule["id"]

            session.run(
                """
                MERGE (r:Rule {ruleId:$id})
                SET r.name = $name,
                    r.platform = $platform,
                    r.description = $desc
                """,
                id=rid,
                name=rule.get("name", rid),
                platform=rule.get("platform", "Unknown"),
                desc=rule.get("description", "")
            )

            _merge_rule_scope_edge(session, rid, rule.get("applies_in"))

            # Triggers
            for trig in rule.get("triggers", []) or []:
                tid = trig["id"]
                session.run(
                    """
                    MATCH (r:Rule {ruleId:$rid})
                    MERGE (t:Trigger {triggerId:$tid})
                    SET t.type = $type,
                        t.operator = $op,
                        t.value = $val
                    MERGE (r)-[:HAS_TRIGGER]->(t)
                    """,
                    rid=rid,
                    tid=tid,
                    type=trig.get("type", "unknown"),
                    op=trig.get("operator"),
                    val=trig.get("value")
                )
                _merge_targets(session, "Trigger", "triggerId", tid, trig.get("targets", []))

            # Conditions
            for cond in rule.get("conditions", []) or []:
                cid = cond["id"]
                session.run(
                    """
                    MATCH (r:Rule {ruleId:$rid})
                    MERGE (c:Condition {conditionId:$cid})
                    SET c.type = $type,
                        c.operator = $op,
                        c.value = $val
                    MERGE (r)-[:HAS_CONDITION]->(c)
                    """,
                    rid=rid,
                    cid=cid,
                    type=cond.get("type", "unknown"),
                    op=cond.get("operator"),
                    val=cond.get("value")
                )
                _merge_targets(session, "Condition", "conditionId", cid, cond.get("targets", []))

            # Actions
            for act in rule.get("actions", []) or []:
                aid = act["id"]
                applies_to = act.get("applies_to_device_id")

                session.run(
                    """
                    MATCH (r:Rule {ruleId:$rid})
                    MERGE (a:Action {actionId:$aid})
                    SET a.type = $type,
                        a.action = $action
                    MERGE (r)-[:HAS_ACTION]->(a)
                    """,
                    rid=rid,
                    aid=aid,
                    type=act.get("type", "unknown"),
                    action=act.get("action")
                )

                if applies_to:
                    session.run(
                        """
                        MATCH (a:Action {actionId:$aid})
                        MATCH (d:Device {deviceId:$did})
                        MERGE (a)-[:APPLIES_TO]->(d)
                        """,
                        aid=aid, did=applies_to
                    )

                _merge_targets(session, "Action", "actionId", aid, act.get("targets", []))

    print(f"✅ Ingestion complete: {yaml_filename}")


# ---------------------------
# Batch ingestion
# ---------------------------
def ingest_all_apartment_files(pattern: str = "smart_building_a*.yaml"):
    files = sorted(BASE_DIR.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No YAML files found matching pattern: {pattern}")

    for path in files:
        ingest_from_yaml(path.name)


if __name__ == "__main__":
    try:
        ingest_all_apartment_files()
    finally:
        _driver.close()