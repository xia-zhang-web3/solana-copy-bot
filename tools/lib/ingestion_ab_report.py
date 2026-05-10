#!/usr/bin/env python3
import json
import sys
from pathlib import Path

from ingestion_ab_report_metrics import (
    evaluate_telemetry,
    load_db_metrics,
    now_utc,
    pct_delta,
)


def append_live_telemetry_checks(
    checks: list[tuple[str, bool, str]],
    control_telemetry: dict,
    candidate_telemetry: dict,
) -> None:
    if not candidate_telemetry.get("available"):
        checks.append(("candidate_telemetry_available", False, "candidate telemetry unavailable"))
        return

    lag_p95_pass_ratio = candidate_telemetry.get("lag_p95_pass_ratio")
    lag_p99_pass_ratio = candidate_telemetry.get("lag_p99_pass_ratio")
    checks.append(
        (
            "lag_p95_gate",
            lag_p95_pass_ratio is not None and lag_p95_pass_ratio >= 0.95,
            f"lag_p95_pass_ratio={lag_p95_pass_ratio}",
        )
    )
    checks.append(
        (
            "lag_p99_gate",
            lag_p99_pass_ratio is not None and lag_p99_pass_ratio >= 0.90,
            f"lag_p99_pass_ratio={lag_p99_pass_ratio}",
        )
    )

    replaced_eval_windows = candidate_telemetry.get("replaced_ratio_eval_windows") or 0
    replaced_pass_ratio = candidate_telemetry.get("replaced_ratio_pass_ratio")
    if replaced_eval_windows > 0:
        checks.append(
            (
                "replaced_ratio_gate",
                replaced_pass_ratio is not None and replaced_pass_ratio >= 0.95,
                f"replaced_ratio_pass_ratio={replaced_pass_ratio}",
            )
        )

    checks.append(
        (
            "reconnect_storm_gate",
            int(candidate_telemetry.get("reconnect_storm_breach_count") or 0) == 0,
            f"reconnect_storm_breach_count={candidate_telemetry.get('reconnect_storm_breach_count')}",
        )
    )
    checks.append(
        (
            "reject_rate_gate",
            int(candidate_telemetry.get("reject_rate_breach_count") or 0) == 0,
            f"reject_rate_breach_count={candidate_telemetry.get('reject_rate_breach_count')}",
        )
    )
    checks.append(
        (
            "no_processed_gate",
            int(candidate_telemetry.get("no_processed_breach_count") or 0) == 0,
            f"no_processed_breach_count={candidate_telemetry.get('no_processed_breach_count')}",
        )
    )

    append_sqlite_amplification_check(
        checks,
        "sqlite_retry_amplification",
        control_telemetry.get("sqlite_write_retry_delta"),
        candidate_telemetry.get("sqlite_write_retry_delta"),
    )
    append_sqlite_amplification_check(
        checks,
        "sqlite_busy_amplification",
        control_telemetry.get("sqlite_busy_error_delta"),
        candidate_telemetry.get("sqlite_busy_error_delta"),
    )


def append_sqlite_amplification_check(
    checks: list[tuple[str, bool, str]],
    name: str,
    control_value,
    candidate_value,
) -> None:
    if control_value is None or candidate_value is None:
        return
    if control_value == 0:
        ok = candidate_value == 0
    else:
        ok = candidate_value <= control_value * 1.2
    checks.append((name, ok, f"control={control_value}, candidate={candidate_value}"))


def render_report(report: dict, checks: list[tuple[str, bool, str]]) -> None:
    print("=== Ingestion A/B Report ===")
    print(f"generated_at_utc: {report['generated_at_utc']}")
    print(f"mode: {report['mode']}")
    print(f"window_minutes: {report['window_minutes']}")
    if report["fixture"]["id"]:
        print(f"fixture_id: {report['fixture']['id']}")
    if report["fixture"]["sha256"]:
        print(f"fixture_sha256: {report['fixture']['sha256']}")
    print(f"git_commit_sha: {report['build']['git_commit_sha']}")
    print(f"control_config_sha256: {report['build']['control_config_sha256']}")
    print(f"candidate_config_sha256: {report['build']['candidate_config_sha256']}")
    print()
    print("-- DB capture --")
    print(
        "control "
        f"observed_buy={report['control']['db']['observed_buy']} "
        f"observed_sell={report['control']['db']['observed_sell']} "
        f"signals_buy={report['control']['db']['signals_buy']}"
    )
    print(
        "candidate "
        f"observed_buy={report['candidate']['db']['observed_buy']} "
        f"observed_sell={report['candidate']['db']['observed_sell']} "
        f"signals_buy={report['candidate']['db']['signals_buy']}"
    )
    print(f"delta observed_buy_pct={report['ab_metrics']['observed_buy_delta_pct']:.2f}")
    print(f"delta observed_sell_pct={report['ab_metrics']['observed_sell_delta_pct']:.2f}")
    print(f"delta signals_buy_pct={report['ab_metrics']['signals_buy_delta_pct']:.2f}")
    print()
    print("-- Checks --")
    for name, ok, detail in checks:
        print(f"[{ 'PASS' if ok else 'FAIL' }] {name}: {detail}")
    print()
    print(f"overall: {'PASS' if report['pass'] else 'FAIL'}")


def main(argv: list[str]) -> int:
    (
        control_db,
        candidate_db,
        window_minutes_raw,
        mode,
        fixture_id,
        fixture_sha256,
        control_log_path,
        candidate_log_path,
        control_service,
        candidate_service,
        output_json,
        buy_target_pct_raw,
        sell_max_degrade_pct_raw,
        control_config,
        candidate_config,
        control_config_sha,
        candidate_config_sha,
        git_commit_sha,
    ) = argv

    window_minutes = int(window_minutes_raw)
    buy_target_pct = float(buy_target_pct_raw)
    sell_max_degrade_pct = float(sell_max_degrade_pct_raw)
    control_db_metrics = load_db_metrics(control_db, window_minutes)
    candidate_db_metrics = load_db_metrics(candidate_db, window_minutes)
    control_telemetry = evaluate_telemetry(control_log_path)
    candidate_telemetry = evaluate_telemetry(candidate_log_path)

    buy_delta_pct = pct_delta(
        float(control_db_metrics["observed_buy"]),
        float(candidate_db_metrics["observed_buy"]),
    )
    sell_delta_pct = pct_delta(
        float(control_db_metrics["observed_sell"]),
        float(candidate_db_metrics["observed_sell"]),
    )
    signal_buy_delta_pct = pct_delta(
        float(control_db_metrics["signals_buy"]),
        float(candidate_db_metrics["signals_buy"]),
    )

    checks: list[tuple[str, bool, str]] = [
        (
            "buy_capture_delta",
            buy_delta_pct >= buy_target_pct,
            f"candidate_observed_buy_delta_pct={buy_delta_pct:.2f} target>={buy_target_pct:.2f}",
        ),
        (
            "sell_capture_non_regression",
            sell_delta_pct >= -sell_max_degrade_pct,
            f"candidate_observed_sell_delta_pct={sell_delta_pct:.2f} allowed>=-{sell_max_degrade_pct:.2f}",
        ),
    ]
    if mode == "live":
        append_live_telemetry_checks(checks, control_telemetry, candidate_telemetry)

    failed_checks = [name for name, ok, _ in checks if not ok]
    report = {
        "generated_at_utc": now_utc().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "mode": mode,
        "window_minutes": window_minutes,
        "fixture": {"id": fixture_id or None, "sha256": fixture_sha256 or None},
        "build": {
            "git_commit_sha": git_commit_sha,
            "control_config": control_config,
            "candidate_config": candidate_config,
            "control_config_sha256": control_config_sha,
            "candidate_config_sha256": candidate_config_sha,
        },
        "control": {
            "db_path": control_db,
            "service": control_service or None,
            "db": control_db_metrics,
            "telemetry": control_telemetry,
        },
        "candidate": {
            "db_path": candidate_db,
            "service": candidate_service or None,
            "db": candidate_db_metrics,
            "telemetry": candidate_telemetry,
        },
        "ab_metrics": {
            "observed_buy_delta_pct": buy_delta_pct,
            "observed_sell_delta_pct": sell_delta_pct,
            "signals_buy_delta_pct": signal_buy_delta_pct,
            "control_observed_buy": control_db_metrics["observed_buy"],
            "candidate_observed_buy": candidate_db_metrics["observed_buy"],
            "control_observed_sell": control_db_metrics["observed_sell"],
            "candidate_observed_sell": candidate_db_metrics["observed_sell"],
        },
        "checks": [
            {"name": name, "ok": ok, "detail": detail}
            for name, ok, detail in checks
        ],
        "pass": not failed_checks,
        "failed_checks": failed_checks,
    }
    render_report(report, checks)
    if output_json:
        out_path = Path(output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(report, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"json_report: {out_path}")
    return 0 if report["pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
