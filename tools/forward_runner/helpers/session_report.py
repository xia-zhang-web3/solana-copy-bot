"""Deterministic final artifacts, called only after the supervisor ends this session."""
from collections import Counter
from contextlib import closing
from decimal import Decimal
import json
import os
import tempfile
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo
import datetime as dt
from session_common import T, L, RUN, read, save
from session_report_data import analyze, read_export, http_manifest, readonly_budget
from session_report_tables import build_tables


def transport_totals(root):
    rows = []
    for path in sorted((Path(root)/'transport').glob('generation-*-backend.json')):
        row = json.loads(path.read_text())
        generation = int(path.name.split('-')[1])
        rows.append(dict(generation=generation, path=str(path),
            **{k:row.get(k) for k in ('received_bytes','upstream_received_bytes','upstream_sent_bytes',
                                    'connection_headroom_bytes','connections','connect_attempts','failures')}))
    if len({r['generation'] for r in rows}) != len(rows):
        raise ValueError('duplicate_generation_stream_measurement')
    observed = sum(r.get('received_bytes') or 0 for r in rows)
    return dict(generations=rows, measurements_available=bool(rows), observed_bidirectional_wire_bytes=observed if rows else None,
        observed_upstream_received_bytes=sum(r.get('upstream_received_bytes') or 0 for r in rows),
        observed_upstream_sent_bytes=sum(r.get('upstream_sent_bytes') or 0 for r in rows),
        connection_headroom_reserved_bytes=sum(r.get('connection_headroom_bytes') or 0 for r in rows),
        stream_cost_at_75_usd_per_decimal_TB=str(Decimal(observed)*Decimal(75)/Decimal(10**12)) if rows else None,
        stream_cost_at_enforced_010_usd_per_GiB=str(Decimal(observed)*Decimal('0.10')/Decimal(2**30)) if rows else None,
        billing_statement=False, transport_direction_accounting='received_bytes counts both observed relay directions once; front not added')


def control_events(path):
    path = Path(path)
    if not path.is_file(): return []
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON')
        return [dict(row) for row in db.execute('SELECT * FROM events ORDER BY id')]


def ingress_gaps(controls):
    gaps=[]
    for row in controls:
        if row['kind']!='ingress_gap_observed':continue
        try:
            payload=json.loads(row['payload']) if isinstance(row['payload'],str) else row['payload']
            if not isinstance(payload,dict):raise ValueError('gap_payload_not_object')
        except (ValueError,TypeError) as exc:payload={'payload_parse_error':type(exc).__name__}
        gaps.append({**payload,'control_event_id':row['id'],'recorded_at':row['ts']})
    return gaps


def readable_time(value):
    if not value: return 'не установлено'
    return dt.datetime.fromisoformat(value.replace('Z','+00:00')).astimezone(ZoneInfo('Europe/Kyiv')).isoformat()


def number(summary, field, subkey=None):
    value = summary.get(field)
    return 'неизвестно' if value is None else str(value[subkey] if subkey else value)


def render(result):
    s=result.get('summary') or {}; budget=result.get('budget') or {}; transport=result['transport']
    lines=[
        f"Фоновый сеанс завершён по причине `{result['stop_reason']}`. Зарегистрировано сигналов: **{number(s,'signals')}**, "
        f"успешных BUY/SELL quotes: **{number(s,'successful_buy_quotes')}/{number(s,'successful_sell_quotes')}**. "
        f"Открытых виртуальных lots: **{number(s,'open_lot_count')}**, открытых или проблемных неоценённых lots: **{number(s,'unvalued_lot_count')}**. "
        "Общий положительный результат не заявлен: незакрытые и проблемные позиции сохранены. Это перспективные котировки, не PnL исполнения.",
        '', '# Автоматический итог', '',
        f"Начало по Киеву: {readable_time(result['schedule'].get('started_at'))}. "
        f"Плановое окончание: {readable_time(result['schedule'].get('ends_at'))}.",
        f"Фактическая остановка и состояния принадлежащих задаче процессов: [STOP_EVIDENCE_REPORT.json](STOP_EVIDENCE_REPORT.json). "
        f"Основание остановки сохранено отдельно от решения о стратегии; завершение таймера не является PASS.",
        '', '## Исправность контура, наблюдения и экономика', '',
        f"Доступность итогового ledger: **{'сохранён' if result['ledger_available'] else 'ошибка чтения / отсутствует'}**. "
        f"Fresh ingress отмечен: **{bool(result['schedule'].get('started_at'))}**. "
        f"Решений Discovery в ledger: **{result.get('cohort_count','неизвестно')}**, допустимых: **{result.get('admissible_cohort_count','неизвестно')}**. "
        f"Ошибок/восстановлений control plane: **{result['control_failure_events']}**; все события сохранены.",
        f"Зафиксировано перерывов сбора: **{len(result['ingress_gaps'])}**. Границы — последнее событие до перерыва и первое после; "
        "они не доказывают отсутствие сделок на рынке. Полная связь с причинами и evidence сохранена в FINAL_RESULTS.json.",
        *[f"- Перерыв: {gap.get('last_before','не установлено')} → {gap.get('first_after','не установлено')}; "
          f"причина: {gap.get('reason',gap.get('payload_parse_error','не установлено'))}."
          for gap in result['ingress_gaps']],
        '',
        "Исправность отдельных компонентов и факт остановки оцениваются по соответствующим доказательствам. "
        "Автоматический отчёт не подменяет итоговую независимую приёмку данных; она остаётся после возвращения владельца.",
        "Достаточность наблюдений для вывода о прибыли не установлена. Не введён новый числовой GREEN-критерий. "
        "Даже положительный условный subtotal закрытий не доказывает устойчивую прибыльность следующего окна или реального исполнения.",
        '', '## Последовательные группы и сигналы', '',
        "[COHORTS.csv](tables/COHORTS.csv) содержит все последовательно опубликованные аналитические решения, "
        "точное фактическое membership available/expires, cutoff/window, fingerprint, причины RED и ссылки на native dry-run. "
        "BUY до готовности решения не добавляется задним числом. Night11 не получили постоянного допуска.",
        "[EVENTS.csv](tables/EVENTS.csv), [JOBS.csv](tables/JOBS.csv), [QUOTES.csv](tables/QUOTES.csv) и "
        "[PROOFS.csv](tables/PROOFS.csv) содержат все сигналы, отказы, пропуски и результаты проверок без отбора удачных строк.",
        f"Observer non-SOL pair count: **{s.get('observer_non_sol_pair_count','неизвестно')}**, отдельно от BUY/SELL сценариев. "
        f"Статусы событий: `{json.dumps(s.get('event_statuses',{}),ensure_ascii=False,sort_keys=True)}`. "
        f"Причины: `{json.dumps(s.get('event_reasons',{}),ensure_ascii=False,sort_keys=True)}`.",
        '', '## Условные закрытия и оставшийся риск', '',
        f"Котируемое поступление по применённым SELL: **{number(s,'closed_quote_proceeds','exact_lamports_fraction')} lamports**; "
        f"относимая на эти выходы стоимость BUY: **{number(s,'closed_allocated_entry_cost','exact_lamports_fraction')} lamports**; "
        f"условная разница до дополнительных fees: **{number(s,'closed_quote_difference_before_network','exact_lamports_fraction')} lamports**.",
        "Этот subtotal включает доказуемые частичные выходы. [ALLOCATIONS.csv](tables/ALLOCATIONS.csv) различает "
        "подготовленное распределение и реально применённую в виртуальном учёте SELL quote; неуспешный exit не закрывает lot.",
        "[LOTS.csv](tables/LOTS.csv), [OPEN_LOTS.csv](tables/OPEN_LOTS.csv) и [UNVALUED_LOTS.csv](tables/UNVALUED_LOTS.csv) "
        "сохраняют остатки, стоимость, происхождение и причины неопределённости. Выбытие из топа не удаляет lot. "
        "Недоказанный знаменатель, прежний остаток, перевод или история аккаунтов не заменяются предположением полного SELL.",
        f"Текущая открытая стоимость: **{number(s,'current_open_cost','exact_lamports_fraction')} lamports**. "
        f"Пик открытой стоимости: **{number(s,'peak_open_cost','exact_lamports_fraction')}**, "
        f"с учётом pending запросов: **{number(s,'peak_open_plus_pending_cost','exact_lamports_fraction')} lamports**. "
        "Это аналитическая потребность, начальный депозит не выдуман; одновременные виртуальные сигналы не означают исполнимость daemon.",
        "Концентрация по уникальному mint и связанным кошелькам: [MINT_CONCENTRATION.csv](tables/MINT_CONCENTRATION.csv). "
        "Один mint нескольких лидеров остаётся одним рынком.",
        '', '## Fees, капитал и расходы провайдеров', '',
        "[FEE_SCENARIOS.csv](tables/FEE_SCENARIOS.csv): на котируемую ногу условно 5 000 / 27 000 / 100 000 lamports. "
        "BUY fee распределяется пропорционально закрытой доле, SELL fee — на каждый котируемый выход. "
        "Это исторические reference/cap сценарии; фактических network/priority fees нет. Priority уже внутри total, "
        "DEX/platform fees в quoted amounts повторно не вычитаются. Дополнительные setup/failed transaction fees не измерены.",
        "Rent reference: 1 488 440 для classic/WSOL account и 1 513 840 для поддерживаемого Token2022 ATA. "
        "Это историческая возвратная потребность в капитале, не автоматически безвозвратный убыток. "
        "Текущие account setup, закрытие/возврат rent и потребность в промежуточном капитале не доказаны.",
        f"Provider budget: **${budget.get('budget_usd','неизвестно')}**; зарезервированная консервативная сумма **${budget.get('reserved_upper_usd','неизвестно')}**, "
        f"остаток **${budget.get('remaining_usd','неизвестно')}**. [PROVIDER_COSTS.csv](tables/PROVIDER_COSTS.csv). "
        "Неиспользованные stream grants не освобождены задним числом; резерв не приравнивается фактическому биллингу.",
        f"Наблюдавшийся двунаправленный wire поток: **{transport['observed_bidirectional_wire_bytes']} bytes**; "
        f"расчёт по $75/decimal TB: **${transport['stream_cost_at_75_usd_per_decimal_TB']}**. "
        "Enforced резерв считает $0.10/GiB и отдельный connection margin. Это разные оценки; выписка провайдера не получена.",
        f"HTTP attempts: **{result['http'].get('attempt_count','неизвестно')}**; сохранённых body bytes: "
        f"**{result['http'].get('body_bytes','неизвестно')}**. Полный [raw-response manifest](RAW_HTTP_MANIFEST.json) "
        "содержит пути/hashes/status, без приватных endpoints и auth headers.",
        '', '## Задержки и границы', '',
        "[LATENCY.csv](tables/LATENCY.csv) отделяет доступные source/ingress/durable timestamps, "
        "observer detection, очередь, начало запроса и получение quote. Durable batch upper bound не выдан за точный "
        "per-event durable timestamp. Detection→quote и HTTP latency не названы полной задержкой leader→follower.",
        "Последовательные quotes не моделируют изменение пула от нашей гипотетической покупки, landing, "
        "подтверждение и успешный выход. Ошибка API не означает нулевую стоимость; отсутствие выхода не удаляет риск.",
        '', f"Пробелы итогового чтения/сохранения: `{json.dumps(result['limitations'],ensure_ascii=False,sort_keys=True)}`.",
        "Машинный итог: [FINAL_RESULTS.json](FINAL_RESULTS.json), полный [ledger export](VIRTUAL_LEDGER_EXPORT.json). "
        "Build NONE, signing/funding/trading/production publication0. Следующий эксперимент автоматически не запускается; "
        "модель после завершения turn не работает.", '']
    return '\n'.join(lines)


def publish_markdown(path, content):
    """Final completion marker appears only after complete, durable report bytes."""
    path = Path(path)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent,
                prefix='.'+path.name+'.', suffix='.tmp', delete=False) as stream:
            temporary = Path(stream.name)
            os.fchmod(stream.fileno(), 0o600)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def generate_final(reason, stop_evidence, ledger_export=None, budget_summary=None):
    limitations=[]; export=None; summary=None; tables={}
    try:
        supplied = ledger_export if ledger_export is not None else read_export(L/'state/virtual.db')
        save(T/'VIRTUAL_LEDGER_EXPORT.json', supplied)
        export, summary = analyze(supplied)
        save(T/'VIRTUAL_LEDGER_EXPORT.json', export)
    except Exception as exc:
        limitations.append('ledger_unavailable:'+type(exc).__name__)
    try:
        manifest = http_manifest(L/'state/http.db', T)
    except Exception as exc:
        manifest=dict(available=False,attempts=[],limitation=type(exc).__name__)
    if not manifest.get('available') or not manifest.get('all_recorded_bodies_verified',False):
        limitations.append('raw_http_manifest_incomplete_or_hash_mismatch')
    save(T/'RAW_HTTP_MANIFEST.json', manifest)
    budget=budget_summary
    if budget is None:
        try: budget=readonly_budget(L/'state/budget.db')
        except Exception as exc: limitations.append('budget_unavailable:'+type(exc).__name__)
    try: transport=transport_totals(T)
    except Exception as exc:
        limitations.append('transport_unavailable:'+type(exc).__name__); transport=transport_totals(Path('/nonexistent-report-empty-root'))
    if not transport['generations']: limitations.append('no_archived_generation_transport_measurements')
    try: controls=control_events(L/'state/control.db')
    except Exception as exc:
        controls=[]; limitations.append('control_events_unavailable:'+type(exc).__name__)
    save(T/'CONTROL_EVENTS_FINAL.json', controls)
    save(T/'STOP_EVIDENCE_REPORT.json', stop_evidence)
    schedule=read(L/'SCHEDULE.json',{})
    if export is not None:
        tables=build_tables(T/'tables',export,summary,manifest,budget)
    result=dict(run_id=RUN,stop_reason=str(reason),schedule=schedule,ledger_available=export is not None,
        summary=summary,cohort_count=len(export['decisions']) if export else None,
        admissible_cohort_count=sum(bool(r['admissible']) for r in export['decisions']) if export else None,
        budget=budget,transport=transport,http={k:v for k,v in manifest.items() if k!='attempts'},tables=tables,
        stop_evidence_path=str(T/'STOP_EVIDENCE_REPORT.json'),
        ingress_gaps=ingress_gaps(controls),
        control_failure_events=sum('failure' in r['kind'] or 'error' in r['kind'] or 'recover' in r['kind'] or 'integrity' in r['kind'] for r in controls),
        limitations=sorted(set(limitations)),overall_positive_result_claimed=False,
        economic_result='UNDETERMINED_EXECUTION_PROFITABILITY',independent_data_review='pending owner return',
        build='NONE',trading=0,production_publication=0,next_experiment_started=False)
    save(T/'FINAL_RESULTS.json',result)
    # Guardian treats this filename as completion; publish it strictly last.
    publish_markdown(T/'FINAL_REPORT_RU.md', render(result))
    return result
