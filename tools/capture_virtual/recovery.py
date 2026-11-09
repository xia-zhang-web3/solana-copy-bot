"""Historical evidence disposition, separate from stream admission and ledger writes."""
import hashlib
import json

from .control import encoded

WSOL = "So11111111111111111111111111111111111111112"
KINDS = {"stream", "complete_live_block", "confirmed_http"}
FORBIDDEN_CLOCKS = {"ts_utc", "source_at", "source_ts", "capture_epoch", "capture_seq", "received_at"}


def disposition_receipt(state, reason, disposition):
    digest = hashlib.sha256()
    for part in (state, reason, disposition):
        raw = part.encode("utf-8")
        digest.update(len(raw).to_bytes(8, "little"))
        digest.update(raw)
    return digest.hexdigest()


def recovery_enabled(control):
    control.require_committed_read()
    names = {r[0] for r in control.db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'capture_recovery%'"
    )}
    if names and not {"capture_recovery", "capture_recovery_outbox", "capture_recovery_evidence", "capture_recovery_links"} <= names:
        raise ValueError("capture_recovery_schema_incomplete")
    return bool(names)


def next_recovery(control):
    with control.lock:
        if not recovery_enabled(control):
            return None
        row = control.db.execute("""SELECT recovery_id,state FROM capture_recovery_outbox
            WHERE state NOT IN ('MISSED','REJECTED') ORDER BY recovery_id LIMIT 1""").fetchone()
        if row and row["state"] != "PENDING":
            raise ValueError("capture_recovery_outcome_unresolved")
        return row["recovery_id"] if row else None


def _facts(row):
    facts = json.loads(row["event_json"])
    if facts is None:
        return None
    if not isinstance(facts, dict) or FORBIDDEN_CLOCKS & facts.keys():
        raise ValueError("capture_recovery_has_stream_clock")
    if (facts.get("signature") != row["signature"] or facts.get("wallet") != row["wallet"]
            or type(facts.get("slot")) is not int or str(facts["slot"]) != row["slot"]):
        raise ValueError("capture_recovery_identity_mismatch")
    return facts


def _swap(facts, token_quantity=None):
    side = ("BUY" if facts["token_in"] == WSOL and facts["token_out"] != WSOL else
            "SELL" if facts["token_out"] == WSOL and facts["token_in"] != WSOL else None)
    if side is None:
        raise ValueError("capture_recovery_pair_not_sol")
    exact = facts.get("exact_amounts") or {}
    qty = exact.get("amount_out_raw" if side == "BUY" else "amount_in_raw")
    decimals = exact.get("amount_out_decimals" if side == "BUY" else "amount_in_decimals")
    mint = facts["token_out"] if side == "BUY" else facts["token_in"]
    if token_quantity is not None:
        if (not isinstance(token_quantity, dict) or token_quantity.get("side") != side
                or token_quantity.get("mint") != mint):
            raise ValueError("capture_recovery_token_quantity_identity_conflict")
        checked_qty, checked_decimals = token_quantity.get("raw"), token_quantity.get("decimals")
        if qty is not None and (qty != checked_qty or decimals != checked_decimals):
            raise ValueError("capture_recovery_token_quantity_amount_conflict")
        qty, decimals = checked_qty, checked_decimals
    if (not isinstance(qty, str) or not qty.isascii() or not qty.isdigit()
            or not 0 < int(qty) < 2**64 or type(decimals) is not int or not 0 <= decimals <= 255):
        raise ValueError("capture_recovery_exact_debit_or_credit_required")
    return side, mint, qty


def canonical_recovery(row):
    facts = _facts(row)
    provenance = json.loads(row["provenance_json"])
    observation = json.loads(row["observation_json"])
    if not isinstance(provenance, dict) or FORBIDDEN_CLOCKS & provenance.keys():
        raise ValueError("capture_recovery_provenance_invalid")
    kind = provenance.get("source_kind")
    if kind not in KINDS:
        raise ValueError("capture_recovery_source_unsupported")
    if hashlib.sha256(bytes(row["raw"])).hexdigest() != row["raw_hash"]:
        raise ValueError("capture_recovery_raw_hash_mismatch")
    if (observation.get("facts") != facts or observation.get("signature") != row["signature"]
            or observation.get("wallet") != row["wallet"] or str(observation.get("slot")) != row["slot"]
            or observation.get("source_kind") != kind
            or observation.get("evidence_sha256") != row["evidence_id"]
            or observation.get("info") != json.loads(row["info_json"])):
        raise ValueError("capture_recovery_observation_binding_mismatch")
    info = json.loads(row["info_json"])
    if bytes(row["raw"]) != bytes(info.get("encoded", [])):
        raise ValueError("capture_recovery_info_bytes_mismatch")
    evidence = provenance.get("evidence")
    if (not isinstance(evidence, dict) or evidence.get("sha256") != row["evidence_id"]
            or evidence.get("source_kind") != kind or str(evidence.get("slot")) != row["slot"]
            or type(evidence.get("available_at_micros")) is not int
            or evidence["available_at_micros"] < 0
            or hashlib.sha256(bytes(row["evidence_raw"])).hexdigest() != row["evidence_id"]
            or row["evidence_raw_hash"] != row["evidence_id"]):
        raise ValueError("capture_recovery_source_evidence_mismatch")
    message_time = observation.get("message_time")
    block = observation.get("block")
    if (provenance.get("message_time") != message_time or provenance.get("block") != block
            or provenance.get("token_quantity") != observation.get("token_quantity")):
        raise ValueError("capture_recovery_provenance_binding_mismatch")
    if kind == "stream":
        # This outbox exists only when the actual stream receive had no current
        # protected admission (for example before a new scope baseline).
        if row["live_seq"] is not None:
            raise ValueError("capture_stream_admission_provenance_conflict")
        valid_time = (message_time == "Missing" or isinstance(message_time, dict)
                      and len(message_time) == 1 and next(iter(message_time)) in
                      {"CreatedAt", "InvalidNanos", "OutOfRangeSeconds"})
        if not valid_time:
            raise ValueError("capture_stream_message_time_invalid")
    else:
        if (not isinstance(message_time, dict) or set(message_time) != {"RecoveredBlock"}
                or message_time["RecoveredBlock"] != {"block_time": evidence.get("block_time")}):
            raise ValueError("capture_recovery_message_time_invalid")
        if (not isinstance(block, dict) or type(block.get("transaction_index")) is not int
                or type(block.get("executed_transaction_count")) is not int
                or not 0 <= block["transaction_index"] < block["executed_transaction_count"]
                or not block.get("blockhash") or not block.get("parent_blockhash")
                or type(block.get("parent_slot")) is not int
                or not 0 <= block["parent_slot"] < int(row["slot"])):
            raise ValueError("capture_recovery_complete_block_identity_invalid")
        if block.get("block_time") != evidence.get("block_time"):
            raise ValueError("capture_recovery_block_binding_mismatch")
    event = dict(version=1, recovery_id=row["id"], signature=row["signature"],
                 wallet=row["wallet"], slot=int(row["slot"]), source_kind=kind,
                 provenance=provenance, block=block, raw_sha256=row["raw_hash"],
                 timeliness="not_established", trading_admission=False,
                 follower_action="none", status="EVIDENCE_ONLY",
                 reason="recovered_non_swap_or_undecodable")
    if facts is not None:
        side, mint, qty = _swap(facts, observation.get("token_quantity"))
        event.update(side=side, mint=mint, token_qty_raw=qty, status="MISSED_UNEXECUTED",
                     reason=("protected_stream_no_admission" if kind == "stream" else
                             "recovered_historical_sell_unexecuted" if side == "SELL" else
                             "recovered_historical_buy_no_admission"))
    return event


def observe_recovery(control, recovery_id):
    if type(recovery_id) is not int or recovery_id <= 0:
        raise ValueError("capture_recovery_id_required")
    with control.atomic():
        row = control.db.execute("""SELECT r.*,e.raw AS evidence_raw,e.raw_hash AS evidence_raw_hash
            FROM capture_recovery r JOIN capture_recovery_evidence e ON e.evidence_id=r.evidence_id
            WHERE r.id=?""", (recovery_id,)).fetchone()
        outbox = control.db.execute("SELECT * FROM capture_recovery_outbox WHERE recovery_id=?",
                                    (recovery_id,)).fetchone()
        if row is None or outbox is None:
            raise ValueError("capture_recovery_outbox_missing")
        event = canonical_recovery(row)
        disposition = encoded(event)
        terminal = "MISSED" if event["status"] == "MISSED_UNEXECUTED" else "REJECTED"
        receipt = disposition_receipt(terminal, event["reason"], disposition)
        if outbox["state"] == terminal:
            if (outbox["disposition_json"] != disposition or outbox["reason"] != event["reason"]
                    or outbox["disposition_sha256"] != receipt):
                raise ValueError("capture_recovery_disposition_conflict")
        elif outbox["state"] != "PENDING":
            raise ValueError("capture_recovery_outcome_unresolved")
        else:
            control.db.execute("""UPDATE capture_recovery_outbox SET state=?,reason=?,
                disposition_json=?,disposition_sha256=?,acknowledged_sha256=NULL
                WHERE recovery_id=? AND state='PENDING'""",
                               (terminal, event["reason"], disposition, receipt, recovery_id))
    # Positive return follows a committed readback. Unknown replies never ACK or
    # call ledger.observe; restart can reconcile this immutable disposition.
    with control.lock:
        try:
            control.require_committed_read()
            actual = control.db.execute("SELECT state,reason,disposition_json,disposition_sha256 FROM capture_recovery_outbox WHERE recovery_id=?",
                                        (recovery_id,)).fetchone()
            if actual is None or tuple(actual) != (terminal, event["reason"], disposition, receipt):
                raise ValueError("capture_recovery_disposition_readback_mismatch")
        except BaseException:
            control._outcome_unknown = True
            raise
    return event


def recovered_live_duplicate(control, seq, event):
    if not recovery_enabled(control):
        return False
    row = control.db.execute("""SELECT r.* FROM capture_recovery r
        JOIN capture_recovery_outbox o ON o.recovery_id=r.id WHERE r.signature=?""",
                             (event["signature"],)).fetchone()
    if row is None:
        return False
    facts = _facts(row)
    if facts is None:
        raise ValueError("capture_recovery_late_stream_facts_conflict")
    side, mint, qty = _swap(facts, json.loads(row["observation_json"]).get("token_quantity"))
    if (row["wallet"] != event["wallet"] or row["slot"] != str(event["slot"])
            or side != event["side"] or mint != event["mint"] or qty != event["token_qty_raw"]):
        raise ValueError("capture_recovery_late_stream_identity_conflict")
    return True
