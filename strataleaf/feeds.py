import copy
from .codec import MISSING, dumps, field, loads, name
from .errors import Conflict, InvalidDocument, NotFound
from .mvcc import checked_revision, latest_row, visible_rows

FIELD_LIMIT = 64


def field_paths(fields):
    """Validate and normalize a field subscription."""
    if isinstance(fields, str) or not isinstance(fields, (list, tuple)) or not 1 <= len(fields) <= FIELD_LIMIT:
        raise InvalidDocument("field subscription requires between 1 and 64 field paths")
    normalized = []
    for path in fields:
        field({}, path)
        if path in normalized:
            raise InvalidDocument("duplicate field path: " + path)
        if any(path.startswith(other + ".") or other.startswith(path + ".") for other in normalized):
            raise InvalidDocument("field paths must not nest inside one another: " + path)
        normalized.append(path)
    return sorted(normalized, key=lambda item: (item.count("."), item))


def project(document, fields):
    """Return the selected part of a document, omitting absent fields."""
    selected = {}
    for path in fields:
        value = field(document, path)
        if value is MISSING:
            continue
        parts = path.split(".")
        target = selected
        for part in parts[:-1]:
            child = target.get(part)
            if not isinstance(child, dict):
                child = {}
                target[part] = child
            target = child
        target[parts[-1]] = copy.deepcopy(value)
    return selected


def _changes(before, after, path=""):
    """Field-level differences between two selected projections.

    Object nodes recurse, so a removed object field is a ``remove`` while an
    explicit JSON null is a ``set`` with ``after`` null; arrays are always
    reported as a whole ``replace``.
    """
    changes = []
    for key in sorted(set(before) | set(after)):
        child = path + "." + key if path else key
        if key not in before:
            changes.append({"path": child, "op": "add", "after": after[key]})
        elif key not in after:
            changes.append({"path": child, "op": "remove", "before": before[key]})
        else:
            left, right = before[key], after[key]
            if isinstance(left, dict) and isinstance(right, dict):
                changes.extend(_changes(left, right, child))
            elif dumps(left) == dumps(right):
                continue
            elif not isinstance(left, (dict, list)) and not isinstance(right, (dict, list)):
                changes.append({"path": child, "op": "set", "before": left, "after": right})
            else:
                changes.append({"path": child, "op": "replace", "before": left, "after": right})
    return changes


def _event(connection, collection, revision, operation, fields):
    """Translate one commit operation into a selected-field document event.

    ``create`` and ``delete`` always describe the whole document lifecycle in
    the collection, while ``update`` carries only differences in the selected
    fields. Because every commit is translated against the version chain, a
    commit that touches the collection is either delivered with all of its
    operations or not delivered at all.
    """
    key, document = operation["key"], operation["document"]
    prior = latest_row(connection, collection, key, revision - 1)
    before = loads(prior["body"]) if prior is not None and prior["body"] is not None else None
    if before is None and document is None:
        return None
    if document is None:
        return {"key": key, "op": "delete"}
    if before is None:
        return {"key": key, "op": "create", "document": project(document, fields)}
    changes = _changes(project(before, fields), project(document, fields))
    return {"key": key, "op": "update", "changes": changes} if changes else None


def _baseline(connection, collection, revision, fields):
    return {"revision": revision,
            "documents": {item["key"]: project(loads(item["body"]), fields)
                          for item in visible_rows(connection, collection, revision)}}


def apply_events(state, events):
    """Rebuild the selected state by applying delivered diffs in revision order.

    Keys are tracked as a complete object map; ``remove`` deletes a selected
    sub-path, ``add``/``set`` write one, ``replace`` swaps an object or array
    node, and ``create``/``delete`` replace or drop the whole document. Passing
    the same event twice is harmless because every event is a whole commit and
    ``create``/``delete`` are absolute rather than incremental.
    """
    state = copy.deepcopy(state)
    for event in events:
        for operation in event["operations"]:
            key = operation["key"]
            if operation["op"] == "delete":
                state.pop(key, None)
            elif operation["op"] == "create":
                state[key] = copy.deepcopy(operation["document"])
            else:
                document = state.setdefault(key, {})
                for change in operation["changes"]:
                    parts = change["path"].split(".")
                    target = document
                    for part in parts[:-1]:
                        target = target.setdefault(part, {})
                    if change["op"] == "remove":
                        target.pop(parts[-1], None)
                    else:
                        target[parts[-1]] = copy.deepcopy(change["after"])
    return state


class ChangeFeed:
    def __init__(self, store):
        self.store = store

    def read(self, since, limit=100):
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise InvalidDocument("feed limit must be between 1 and 1000 commits")
        with self.store.read() as connection:
            checked_revision(self.store, since)
            return [loads(row[0]) for row in connection.execute("SELECT payload FROM commits WHERE rev>? ORDER BY rev LIMIT ?", (since, limit))]

    def register(self, consumer, since=None):
        name(consumer)
        with self.store.atomic() as connection:
            since = checked_revision(self.store, since)
            if connection.execute("SELECT 1 FROM consumers WHERE name=?", (consumer,)).fetchone():
                raise Conflict("consumer already registered")
            connection.execute("INSERT INTO consumers(name,ack) VALUES (?,?)", (consumer, since))
        return since

    def register_diff(self, consumer, collection, fields, since=None):
        """Register a consumer that receives selected-field diffs for one collection."""
        name(consumer)
        name(collection)
        fields = field_paths(fields)
        with self.store.atomic() as connection:
            since = checked_revision(self.store, since)
            if not connection.execute("SELECT 1 FROM collections WHERE name=?", (collection,)).fetchone():
                raise NotFound("unknown collection: " + collection)
            if connection.execute("SELECT 1 FROM consumers WHERE name=?", (consumer,)).fetchone():
                raise Conflict("consumer already registered")
            generation = connection.execute("SELECT generation FROM collections WHERE name=?",
                                            (collection,)).fetchone()[0]
            connection.execute("""INSERT INTO consumers(name,ack,collection,fields,baseline_rev,schema_generation)
                                  VALUES (?,?,?,?,?,?)""",
                               (consumer, since, collection, dumps(fields), since, generation))
            baseline = _baseline(connection, collection, since, fields)
        return {"consumer": consumer, "collection": collection, "fields": fields,
                "baseline_revision": since, "ack": since, "baseline": baseline}

    def read_diff(self, consumer, limit=100, baseline=False):
        """Deliver whole commits as selected-field diffs, with an initial baseline.

        The first delivery, and any delivery that asks for ``baseline``, carries
        the complete selected state at the consumer's acknowledgement revision so
        a consumer joining inside the retained window never sees a diff without
        the state it applies to. ``limit`` bounds commits, never operations, so a
        multi-document commit is delivered whole or not at all.
        """
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise InvalidDocument("diff limit must be between 1 and 1000 commits")
        with self.store.read() as connection:
            row = connection.execute("SELECT * FROM consumers WHERE name=?", (consumer,)).fetchone()
            if row is None:
                raise NotFound("unknown consumer")
            if row["fields"] is None:
                raise InvalidDocument("consumer is not a field subscription")
            fields, collection = loads(row["fields"]), row["collection"]
            ack = checked_revision(self.store, row["ack"])
            if not connection.execute("SELECT 1 FROM collections WHERE name=?", (collection,)).fetchone():
                raise NotFound("unknown collection: " + collection)
            rows = connection.execute("SELECT rev,payload FROM commits WHERE rev>? ORDER BY rev LIMIT ?",
                                      (ack, limit + 1)).fetchall()
            more = len(rows) > limit
            rows = rows[:limit]
            events = []
            for item in rows:
                commit = loads(item["payload"])
                operations = [event for event in
                              (_event(connection, collection, item["rev"], operation, fields)
                               for operation in commit["operations"] if operation["collection"] == collection)
                              if event is not None]
                if operations:
                    events.append({"revision": item["rev"], "operations": operations})
            definition = connection.execute("SELECT * FROM collections WHERE name=?", (collection,)).fetchone()
            schema_changed = definition["generation"] != row["schema_generation"]
            result = {"consumer": consumer, "collection": collection, "fields": fields,
                      "baseline_revision": row["baseline_rev"], "ack": ack,
                      "through": rows[-1]["rev"] if rows else ack, "more": more,
                      "catalog_epoch": int(self.store.get("catalog_epoch")),
                      "schema_generation": definition["generation"],
                      "schema_changed": schema_changed,
                      "baseline": _baseline(connection, collection, ack, fields)
                      if baseline or schema_changed or ack == row["baseline_rev"] else None,
                      "events": events}
            if schema_changed:
                result["schema"] = {"name": definition["name"], "schema": loads(definition["schema"]),
                                    "generation": definition["generation"]}
            return result

    def snapshot(self, consumer):
        """Return the selected state at the consumer's current acknowledgement."""
        with self.store.read() as connection:
            row = self._subscription(connection, consumer)
            if row is None:
                raise NotFound("unknown consumer")
            collection, fields, ack = row
            if not connection.execute("SELECT 1 FROM collections WHERE name=?", (collection,)).fetchone():
                raise NotFound("unknown collection: " + collection)
            return {"consumer": consumer, "collection": collection, "fields": fields,
                    "baseline_revision": connection.execute("SELECT baseline_rev FROM consumers WHERE name=?",
                                                            (consumer,)).fetchone()[0],
                    "ack": ack, "baseline": _baseline(connection, collection, ack, fields)}

    def rebuild(self, consumer, limit=1000):
        """Rebuild the selected state from the consumer baseline and later events.

        The baseline is read at the consumer's acknowledgement revision, then
        every available commit is translated and applied, so the result explains
        its own provenance. Delivery is idempotent: rerunning after the same
        acknowledgement yields the same state, and a partially acknowledged
        range replays only the commits that were not acknowledged.
        """
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise InvalidDocument("rebuild limit must be between 1 and 1000 commits")
        with self.store.read() as connection:
            row = self._subscription(connection, consumer)
            if row is None:
                raise NotFound("unknown consumer")
            collection, fields, ack = row
            if not connection.execute("SELECT 1 FROM collections WHERE name=?", (collection,)).fetchone():
                raise NotFound("unknown collection: " + collection)
            rows = connection.execute("SELECT rev,payload FROM commits WHERE rev>? ORDER BY rev LIMIT ?",
                                      (ack, limit + 1)).fetchall()
            more = len(rows) > limit
            events = []
            for item in rows[:limit]:
                commit = loads(item["payload"])
                operations = [event for event in
                              (_event(connection, collection, item["rev"], operation, fields)
                               for operation in commit["operations"] if operation["collection"] == collection)
                              if event is not None]
                if operations:
                    events.append({"revision": item["rev"], "operations": operations})
            baseline = _baseline(connection, collection, ack, fields)
            through = rows[min(limit, len(rows)) - 1]["rev"] if rows else ack
            documents = apply_events(baseline["documents"], events)
            return {"consumer": consumer, "collection": collection, "fields": fields,
                    "baseline_revision": ack, "revision": through, "more": more,
                    "documents": documents, "events": len(events)}

    def _subscription(self, connection, consumer):
        row = connection.execute("SELECT * FROM consumers WHERE name=?", (consumer,)).fetchone()
        if row is None:
            return None
        if row["fields"] is None:
            raise InvalidDocument("consumer is not a field subscription")
        return row["collection"], loads(row["fields"]), checked_revision(self.store, row["ack"])

    def position(self, consumer):
        with self.store.read() as connection:
            row = connection.execute("SELECT ack FROM consumers WHERE name=?", (consumer,)).fetchone()
            if row is None:
                raise NotFound("unknown consumer")
            return row[0]

    def acknowledge(self, consumer, revision):
        with self.store.atomic() as connection:
            revision = checked_revision(self.store, revision)
            row = connection.execute("SELECT * FROM consumers WHERE name=?", (consumer,)).fetchone()
            if row is None:
                raise NotFound("unknown consumer")
            if revision < row["ack"]:
                raise Conflict("consumer acknowledgement cannot move backwards")
            connection.execute("UPDATE consumers SET ack=? WHERE name=?", (revision, consumer))
            if row["fields"] is not None:
                # A schema change stays visible until an acknowledgement covers it.
                definition = connection.execute("SELECT generation, schema_rev FROM collections WHERE name=?",
                                                (row["collection"],)).fetchone()
                if definition is None or revision >= definition["schema_rev"]:
                    connection.execute("UPDATE consumers SET schema_generation=? WHERE name=?",
                                       (definition["generation"] if definition else None, consumer))
        return revision

    def release(self, consumer):
        with self.store.atomic() as connection:
            if not connection.execute("DELETE FROM consumers WHERE name=?", (consumer,)).rowcount:
                raise NotFound("unknown consumer")
