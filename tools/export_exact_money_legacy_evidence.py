#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import sqlite3
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export legacy approximate exact-money evidence into durable CSV artifacts "
            "before any later pruning."
        )
    )
    parser.add_argument("db_path", help="Path to SQLite database")
    parser.add_argument(
        "output_dir",
        help="Directory where CSV, summary, and manifest artifacts will be written",
    )
    return parser.parse_args()


def resolve_db_path(raw: str) -> Path:
    path = Path(raw).expanduser()
    if not path.is_file():
        raise SystemExit(f"sqlite db not found: {raw}")
    return path


def resolve_output_dir(raw: str) -> Path:
    path = Path(raw).expanduser()
    path.mkdir(parents=True, exist_ok=True)
    if not path.is_dir():
        raise SystemExit(f"output path is not a directory: {raw}")
    return path


def load_exact_money_coverage_module():
    module_path = Path(__file__).with_name("exact_money_coverage_report.py")
    spec = importlib.util.spec_from_file_location(
        "exact_money_coverage_report", module_path
    )
    if spec is None or spec.loader is None:
        raise SystemExit(f"failed loading exact money coverage module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def write_csv(
    conn: sqlite3.Connection,
    output_path: Path,
    select_sql: str,
    predicate: str,
    order_by_sql: str,
) -> int:
    query = f"{select_sql} WHERE {predicate} ORDER BY {order_by_sql}"
    cursor = conn.execute(query)
    headers = [column[0] for column in cursor.description or ()]
    row_count = 0
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        for row in cursor:
            writer.writerow(row)
            row_count += 1
    return row_count


def main() -> int:
    args = parse_args()
    db_path = resolve_db_path(args.db_path)
    output_dir = resolve_output_dir(args.output_dir)

    coverage = load_exact_money_coverage_module()
    conn = sqlite3.connect(str(db_path))
    try:
        report = coverage.collect_report(conn)
        if report.cutover is None:
            raise SystemExit("exact money cutover marker is missing; cannot export legacy evidence")

        surface_rows: list[tuple[str, int, int, Path, str]] = []
        legacy_approximate_rows_total = 0
        post_cutover_approximate_rows_total = 0

        for surface in coverage.legacy_evidence_surfaces():
            coverage_entry = getattr(report, surface.name, None)
            if coverage_entry is None:
                raise SystemExit(f"missing coverage entry for surface: {surface.name}")
            if (
                coverage_entry.legacy_approximate_rows is None
                or coverage_entry.post_cutover_rows is None
                or coverage_entry.post_cutover_exact_rows is None
            ):
                raise SystemExit(
                    f"coverage unavailable for legacy export surface: {surface.name}"
                )
            legacy_rows = int(coverage_entry.legacy_approximate_rows)
            post_cutover_approximate_rows = int(
                coverage_entry.post_cutover_rows - coverage_entry.post_cutover_exact_rows
            )
            legacy_approximate_rows_total += legacy_rows
            post_cutover_approximate_rows_total += post_cutover_approximate_rows

            output_path = output_dir / f"exact_money_legacy_{surface.name}.csv"
            exported_rows = write_csv(
                conn,
                output_path,
                surface.export_select_sql,
                coverage.legacy_approximate_predicate(
                    surface, report.cutover.cutover_ts
                ),
                surface.order_by_sql,
            )
            if exported_rows != legacy_rows:
                raise SystemExit(
                    f"legacy export row mismatch for {surface.name}: "
                    f"exported={exported_rows} expected={legacy_rows}"
                )
            surface_rows.append(
                (
                    surface.name,
                    legacy_rows,
                    post_cutover_approximate_rows,
                    output_path,
                    sha256_path(output_path),
                )
            )
    finally:
        conn.close()

    verdict = "PASS"
    reason_code = "legacy_evidence_exported"
    if post_cutover_approximate_rows_total > 0:
        verdict = "WARN"
        reason_code = "post_cutover_approximate_detected"

    summary_path = output_dir / "exact_money_legacy_export_summary.txt"
    summary_lines = [
        "=== Exact Money Legacy Evidence Export ===",
        f"db: {db_path}",
        f"output_dir: {output_dir}",
        "exact_money_cutover_present: yes",
        f"exact_money_cutover_ts: {report.cutover.cutover_ts}",
        f"exact_money_cutover_recorded_ts: {report.cutover.recorded_ts}",
        f"exact_money_cutover_note: {report.cutover.note or 'n/a'}",
        f"exact_money_legacy_export_verdict: {verdict}",
        f"exact_money_legacy_export_reason_code: {reason_code}",
        f"legacy_approximate_rows_total: {legacy_approximate_rows_total}",
        f"post_cutover_approximate_rows_total: {post_cutover_approximate_rows_total}",
    ]
    for name, legacy_rows, post_cutover_rows, output_path, sha256 in surface_rows:
        summary_lines.extend(
            [
                f"{name}_legacy_approximate_rows: {legacy_rows}",
                f"{name}_post_cutover_approximate_rows: {post_cutover_rows}",
                f"artifact_{name}_legacy_csv: {output_path}",
                f"{name}_legacy_csv_sha256: {sha256}",
            ]
        )
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    summary_sha256 = sha256_path(summary_path)

    manifest_path = output_dir / "exact_money_legacy_export_manifest.txt"
    manifest_lines = [
        f"summary_path={summary_path}",
        f"summary_sha256={summary_sha256}",
    ]
    for name, legacy_rows, post_cutover_rows, output_path, sha256 in surface_rows:
        manifest_lines.append(
            f"{name}|legacy_rows={legacy_rows}|post_cutover_approximate_rows={post_cutover_rows}|"
            f"path={output_path}|sha256={sha256}"
        )
    manifest_path.write_text("\n".join(manifest_lines) + "\n", encoding="utf-8")
    manifest_sha256 = sha256_path(manifest_path)

    print("\n".join(summary_lines))
    print(f"artifact_summary: {summary_path}")
    print(f"summary_sha256: {summary_sha256}")
    print(f"artifact_manifest: {manifest_path}")
    print(f"manifest_sha256: {manifest_sha256}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
