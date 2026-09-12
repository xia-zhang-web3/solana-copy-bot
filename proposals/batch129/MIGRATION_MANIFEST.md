# №129: shipped migrations и startup contract

Все 79 shipped SQL ниже — exact accepted127 bytes/modes, ordered по filename,
как `crates/storage-core/src/migrations.rs`. 0077 и0078 сохранены; это полный
bundle, а не утверждение о двух pending migrations на неизвестном сервере.

## Полный ordered manifest

| # | Shipped path / schema_migrations.version = basename | Mode | SHA256 |
|---|---|---|---|
| 1 | `migrations/0001_init.sql` | 0o644 | `f5faa163e8960b36d5c871e1cdb4bb85223eb25518f14e4e47afd65b7d803911` |
| 2 | `migrations/0002_shadow_phase2.sql` | 0o644 | `07c8fd19fbec75a35cdf64c0a2d4e5025b7fa0db1373ab99501ad79c696f994d` |
| 3 | `migrations/0003_token_quality_indexes.sql` | 0o644 | `b877957e96e41fb51265e7bb7c779b632ed7648622bca72ab6c9281c68e338d3` |
| 4 | `migrations/0004_wallet_metrics_quality_phaseb.sql` | 0o644 | `e6bd3b8afc859bbd2e53aa7393900ecd6b509371afe2ac5301f38718455e1207` |
| 5 | `migrations/0005_token_quality_cache.sql` | 0o644 | `ae9bc266f0c191d756c792d56682b29036a48f1f3393b8a89a1fcc2f323a83ce` |
| 6 | `migrations/0006_followlist_temporal_index.sql` | 0o644 | `2318481eaed464b3f98db8ca0e44d0b2003c52198755bccbe779a85a54dcd08a` |
| 7 | `migrations/0007_observed_swaps_time_scan_index.sql` | 0o644 | `969692accc94bbad2ed763caaf3c717a318b031f977db60de4c89b7a9e96c72a` |
| 8 | `migrations/0008_shadow_perf_indexes.sql` | 0o644 | `4df53a2fb40b65c351052f11868c2593cc142ae58e2d5604f9471d0157618b54` |
| 9 | `migrations/0009_execution_lifecycle.sql` | 0o644 | `4e6ea3236a317865c2d7451caa726d37544b4b695f3f8d6b4af117a7a76c06af` |
| 10 | `migrations/0010_execution_safety.sql` | 0o644 | `9493c63db9fc5edf20615d72553d5afd4dabbb974ce820ccc9790eaf94ada748` |
| 11 | `migrations/0011_execution_fill_uniqueness.sql` | 0o644 | `dff18df2dd166c96eed3d9e25a9ad8ae9fafa41bbec3364c0df5d3c37ec306d8` |
| 12 | `migrations/0012_execution_fill_index_cleanup.sql` | 0o644 | `d9a26ea43893665f8cf1c2c9f7ea4d626d6e6e5ef43a1195e5d3ccdc9d49cc59` |
| 13 | `migrations/0013_execution_fee_breakdown.sql` | 0o644 | `c91722b68441389c991eadb51b8ab5eeb69ee58b7df17b477463632adfafa457` |
| 14 | `migrations/0014_execution_fee_breakdown_safety.sql` | 0o644 | `dfb9c1515546e76a218449ee90ab34e210155d4234beedca893e706dff7a50fb` |
| 15 | `migrations/0015_execution_fee_breakdown_hints.sql` | 0o644 | `e669cd7ad30626203cc2feba03310011a049c8b6ba80f40ce1276297c57c961b` |
| 16 | `migrations/0016_execution_fee_breakdown_hints_safety.sql` | 0o644 | `57335faf4bcd9d55315931c741c8d6c2916c1405c55526b204ede6caea131659` |
| 17 | `migrations/0017_positions_closed_state_index.sql` | 0o644 | `36375ce81ba1297fe3ee911599816da45909eded7d71bd2804db7fe8402475ab` |
| 18 | `migrations/0018_followlist_single_active_guard.sql` | 0o644 | `f8cf1536b413690999af266a32cedda0ff0bc93d6c4349829130efe0beb6132c` |
| 19 | `migrations/0019_positions_single_open_guard.sql` | 0o644 | `2a87a522b5aa5fac466b503b6bda4ace2f26a946a7a114f8556331f9f3e5da5e` |
| 20 | `migrations/0020_execution_foreign_keys.sql` | 0o644 | `e8ac79ce4e50f2a2928f47aab2e73df152c7d9fdf4858801532edd2257a4e0fb` |
| 21 | `migrations/0021_wallet_metrics_window_start_index.sql` | 0o644 | `eaeb44473afcdb1387640b01735ca3f07027cf23365ac226a2c68a3c0362053c` |
| 22 | `migrations/0022_alert_delivery_state.sql` | 0o644 | `c1bf00046593996ae3e76fe721ed67c244b99a7360495caab801bc4c2bfcb5ac` |
| 23 | `migrations/0022_trusted_wallet_metrics_snapshots.sql` | 0o644 | `92410229422ec73cb24e5061c2a3aa29984368880e5b7e9a7e404f58a130324a` |
| 24 | `migrations/0023_observed_swaps_exact_amounts.sql` | 0o644 | `0ce1b0a5986bc89018ee529a1712c3d02f18ee1e38408d1b6ea4ff0c7b992f34` |
| 25 | `migrations/0024_execution_exact_lamports.sql` | 0o644 | `fdb07d2c43ff2f914c2a664e4bc90c445af7d5494e0c7f0d902ed69fac5125d9` |
| 26 | `migrations/0025_shadow_exact_lamports.sql` | 0o644 | `8d3657c760f49c9e94724dff37a82242be0215e483c5d18bde4277a32f5b1d29` |
| 27 | `migrations/0026_wallet_activity_days.sql` | 0o644 | `8184fc58a3ef3f2844aa6d13da98004f20a1edb9707ae1a25863e795542d324d` |
| 28 | `migrations/0027_execution_exact_quantities.sql` | 0o644 | `91b84bdd0d0ff163e1d4aac6be1f2c3e14d1715018555c89c66203e2167c59a2` |
| 29 | `migrations/0028_execution_pnl_lamports.sql` | 0o644 | `c35542a976c31b3c74d5564354c3450fbac615e2fd12f284bcfee8396d7816d4` |
| 30 | `migrations/0029_exact_money_cutover_state.sql` | 0o644 | `4c2149890f53ace3ba1cb4dd38a863b91aad77c1a4acbbb5c0490d7231f923a4` |
| 31 | `migrations/0030_shadow_exact_quantities.sql` | 0o644 | `70d0cd77b575c8d652b3a123230d7451ef175c9dbe34ffc6457c62d748d4e0e9` |
| 32 | `migrations/0031_copy_signals_exact_notional.sql` | 0o644 | `bdb30362ddc5016b30b8c0a989734b31f73205be25ea5477e300873e811e0e9b` |
| 33 | `migrations/0032_copy_signals_notional_origin.sql` | 0o644 | `49e7646a36fda9644bbb4b50e9a31901e76e91eac9d05096dfe212834cd05cab` |
| 34 | `migrations/0033_positions_accounting_bucket.sql` | 0o644 | `74a9426a99dec1f44cbd06fe126ea5ce9a5619df3862b678c6d89107a5d558a7` |
| 35 | `migrations/0034_shadow_accounting_bucket.sql` | 0o644 | `f5383a1acd3054cebef040a102057392dd13baf029e29dda9d84d6e284d1b459` |
| 36 | `migrations/0035_discovery_scoring_aggregates.sql` | 0o644 | `685fda74e4abbf49bbc8d590ea7d3b93f7d402d1609bb78e8a9ba1489c3fdc6b` |
| 37 | `migrations/0036_shadow_close_context.sql` | 0o644 | `d427fc8cf7461a5e3f57d5bd8e1cb173e387a0977faf679bdb3b86c26a98fb34` |
| 38 | `migrations/0037_risk_events_type_index.sql` | 0o644 | `cf98f3f1c06d6deab9f18af0fd80e94aa8c867ece147f2cbe387e91f3293ad5a` |
| 39 | `migrations/0038_shadow_lot_risk_context.sql` | 0o644 | `543d686e79ee5778ea1be6b93f133cf506ae9e4192df3bfd6b4b9d699d732d66` |
| 40 | `migrations/0039_observed_swaps_sol_leg_ts_index.sql` | 0o644 | `0cc9f59ad69743d8f1ee3b2004a77dee026151a1987e988c0dfa6649c721aa8b` |
| 41 | `migrations/0040_observed_swaps_non_utc_ts_index.sql` | 0o644 | `609a9aedc663acc711918d9068d3bcb2940a456b14172a4bc548fdb56c0f800f` |
| 42 | `migrations/0041_discovery_v2_status_snapshot.sql` | 0o644 | `3752f262c4e3550ef859428bf72599ebf61cf1abb14bd201e54dc5a689fc8437` |
| 43 | `migrations/0042_observed_sol_leg_projection.sql` | 0o644 | `45d5f2472bfb8a1eac27a3eb8ba4853afa3100d6c9210275cc2ad8f671f5337e` |
| 44 | `migrations/0043_observed_sol_leg_projection_covering_index.sql` | 0o644 | `b6c6f8122f6c38cf57db5980793642febc22b1f33df26d8586078e4440dd55e0` |
| 45 | `migrations/0044_execution_quote_canary_events.sql` | 0o644 | `5d471aa86f4a2c242070bbd07e73591062abb10a18ace36abcef9c50d05f1fbd` |
| 46 | `migrations/0045_execution_quote_canary_decision.sql` | 0o644 | `6eb2720722176ef36dd8b3d69cf4d75ad4515d8fc898661af4d8e1f3a68483f2` |
| 47 | `migrations/0046_execution_quote_canary_provider_samples.sql` | 0o644 | `fdcfba7cea85204181be609766542a5d02771dc2965e5745facbc4b2c4c94a94` |
| 48 | `migrations/0047_execution_quote_canary_shadow_gate.sql` | 0o644 | `b51895a4d9bcee89c0dcaf24eada8200546f8ff11c6e98163dc33bd7c91021c9` |
| 49 | `migrations/0048_execution_canary_build_plan_metadata.sql` | 0o644 | `c1cd30acaded1b61af75e81285d70ddae316dcb4751414f11d6d808902daf26a` |
| 50 | `migrations/0049_execution_quote_canary_discovery_rank_stamp.sql` | 0o644 | `9e63846257cfd76e8ea3bb7b0019f37994726cc6daaae1f2eb94355e21069947` |
| 51 | `migrations/0050_discovery_candidate_source_cohort.sql` | 0o644 | `8cbc41011bdd91b55039d01f58b663b58244a6a077dc3884125ee10c2c0fc9ac` |
| 52 | `migrations/0051_execution_canary_receipt_proofs.sql` | 0o644 | `c90267b9bd5d310d2a65216212c320fb8fa34f1590998c376a912b699327def9` |
| 53 | `migrations/0052_shadow_close_signal_index.sql` | 0o644 | `c6ce63549e3d9f398c9c468872c3469632656d3bc05a87bf62a3a0095d6fa4d1` |
| 54 | `migrations/0053_execution_owned_sell_cursor.sql` | 0o644 | `3844b883c4045948865cd4bcf666d70386cb6e885e467bf8abc8779bb7283d83` |
| 55 | `migrations/0054_execution_canary_receipt_facts.sql` | 0o644 | `706f50f9d10eea9d8284fcc2d4b8fc1a1593d0f3701f9db5e855fb512219fe3f` |
| 56 | `migrations/0055_execution_receipt_cash_settlement.sql` | 0o644 | `cf9245a29a2ac563d60b71b2e91e8be76962270f6b2ec9ff811d54edbbb527e5` |
| 57 | `migrations/0056_execution_failed_expenses.sql` | 0o644 | `87b5aa310bd160badb05fca67116cb2242f5e6d74da80e6172ae33e7bb4fe395` |
| 58 | `migrations/0057_execution_receipt_native_observations.sql` | 0o644 | `97be9c918132b75a3ca06fa831c2b02738ca4e4e315ff086558363823ceeeb5d` |
| 59 | `migrations/0058_execution_source_sell_intents.sql` | 0o644 | `5794e2f707eeedf55935d7372a748b6fbc8e5b31b2f678faa6ee279f507046ee` |
| 60 | `migrations/0059_execution_source_sell_promotions.sql` | 0o644 | `78ef3c961d2f645d8956bd8dac441bfe23837ea1974a66f8bd8d4ee6ef16f0f4` |
| 61 | `migrations/0060_execution_failed_sell_sweep_cursor.sql` | 0o644 | `29f3cc03f7c56a77a4147a9ea33ea788bc27764e6e9bd525e26c4b4e1aeb3e58` |
| 62 | `migrations/0061_buy_receipt_ownership_indexes.sql` | 0o644 | `e3cd70e8c4de4c452f257987d0ebd60790835d1eb1471b1d31124e31c168eb33` |
| 63 | `migrations/0062_execution_source_sell_staging_cursor.sql` | 0o644 | `9896a2abc615c66c5db6e76c637b03b14a71a5789f40f5d61bb18aa2c94341da` |
| 64 | `migrations/0063_observed_retention_boundary.sql` | 0o644 | `c0958552a31f512b26c9f29b05ccef26d0a1f3e55c0c7fc080a79e82111f2d13` |
| 65 | `migrations/0064_execution_canary_dispatch.sql` | 0o644 | `41fe581813b9230ff8ec251481974fcc5315f53acc9e020aef31ff0be56c2c82` |
| 66 | `migrations/0065_source_sell_handoff.sql` | 0o644 | `7b1be791a9064c70816f4a2da4eee3790c2b34ff58ede4a9f238cdb9a449a8a5` |
| 67 | `migrations/0066_quote_http_timing.sql` | 0o644 | `cccdd3e3dc9d303750a9c6cb5f8bcf5f37e51c2d34560771f51c3a29a20f408a` |
| 68 | `migrations/0067_association_inbox.sql` | 0o644 | `923f1978c52b2ab673c330ad14176cef316e4e3a473368d220c9c567c0bbf6f6` |
| 69 | `migrations/0068_association_sell_preparation.sql` | 0o644 | `3d709ef6b7fa84603dc59752e4cc779fcf174f1da7ebc111dc07c09abb443b07` |
| 70 | `migrations/0069_association_parent_graph.sql` | 0o644 | `8d07a39347ac3932ed75954d4fd525112299eedbc342b1cee9fe59c4b6870c49` |
| 71 | `migrations/0070_owned_sell_amount_proof.sql` | 0o644 | `b1afa4b23f82162b54b612ce1c26ff5f9ac8c8005df04ddafee73775a559425a` |
| 72 | `migrations/0071_shadow_lot_origins.sql` | 0o644 | `5da89fe8e04f9b0ce3169ecf0a5c4d269da0513b05e65951cf1fd9475176826c` |
| 73 | `migrations/0072_ordered_source_sell_intents.sql` | 0o644 | `f25e057cb27ae4cf7c70d6fdab0f125d7aa025529b96240b75ff4196ed4a5a86` |
| 74 | `migrations/0073_association_budget_indexes.sql` | 0o644 | `30c5397f2f129b3a2cdaa3d4dccdf2a9cbb9e716180f4a2804cc58e3dc0c0c0b` |
| 75 | `migrations/0074_ordered_sell_quote_only.sql` | 0o644 | `3d8741bb6b8da62e55d2c62826627e3d53143b03d36aed4d23fa23fc75f3979d` |
| 76 | `migrations/0075_shadow_sell_recovery.sql` | 0o644 | `3d19359bca24415af47a7b64c1b4b161a926247a018371f8c363714c02a116d0` |
| 77 | `migrations/0076_quote_response_availability.sql` | 0o644 | `0ac853be08a33dd2f508f4bd56637b52d78e348a8181114a342aa39b9e300c38` |
| 78 | `migrations/0077_tiny_experiment_budget.sql` | 0o644 | `d18eb1a954c81013737a635d500dd73540f50ada2e1ffab4098b026935c33f90` |
| 79 | `migrations/0078_tiny_protected_native_capital.sql` | 0o644 | `b0ba6e2a1182e61ebc05b865431c13a4c22ab6a4472afa956ef092ee1380e977` |


## Фактическое применение на startup

`app_main.rs`: config/env load и validation → resolve_migrations_dir →
`SqliteStore::open_and_migrate_for_startup` из storage-core → дальнейший runtime.
Открытие store/ensure-schema уже может писать SQLite; false execution flags
не превращают startup в read-only процесс. Поэтому backups нужны до writer start.

Resolver (`runtime_bootstrap.rs`): absolute path или существующий configured path
от WorkingDirectory; иначе sibling config path, затем parent config directory;
иначе возвращается configured path и missing dir приводит к ошибке. Actual
`system.migrations_dir` и symlink должны связывать именно verified app bundle.

Storage-core читает top-level .sql, сортирует paths, разделяет startup-deferred
индексы и blocking migrations. Для pending blocking SQL одна IMMEDIATE transaction:
lookup schema_migrations.version по полному filename; recorded version пропускается;
SQL + запись version/applied_at, semantic checks и commit. Ошибка abort/rollback
migration transaction; весь startup, включая open/schema/index preparation, не
объявляется одной атомарной транзакцией. Особая fill-cash migration 0055 сохраняет
существующий foreign-key/rebuild contract. Version table не хранит SQL checksum:
совпавший filename сам по себе не доказывает bytes исторически применённого SQL.

Шесть специальных deferred candidates:
0003_token_quality_indexes.sql, 0007_observed_swaps_time_scan_index.sql,
0008_shadow_perf_indexes.sql, 0039_observed_swaps_sol_leg_ts_index.sql,
0040_observed_swaps_non_utc_ts_index.sql,
0043_observed_sol_leg_projection_covering_index.sql.
Apply/Defer/RecordSatisfied зависит от actual tables/rows/index definitions/recorded
state; missing/invalid индексы на populated data могут оставаться deferred, а
уже удовлетворённые записываться отдельно. Не считать startup гарантией применения
всех79 за один проход; deferred timestamp-sensitive surfaces остаются fail-closed.

0077 создаёт durable experiment/reservations без activation. 0078 добавляет
policy_mode с decoded_amount default, pinned native policy/capital evidence,
пересоздаёт reservations с nullable buy_lamports и переносит старые columns.
Migration не конвертирует/не активирует старый experiment и не обнуляет holds.

## Будущий server schema preflight, сейчас не исполнять

`DB_PATH` — actual loaded database, `SCHEMA_OUTPUT` — private local evidence output
будущего разрешённого preflight. Connection read-only, одна transaction, deadline,
нет schema creation/checkpoint/repair. Не использовать immutable=1 для live WAL.

```bash
: "${DB_PATH:?actual database path from approved service config}"
: "${SCHEMA_OUTPUT:?private output path for schema inventory}"
timeout 15s python3 - "$DB_PATH" > "$SCHEMA_OUTPUT" <<'SQL_READ'
import json, pathlib, sqlite3, sys, time
p = pathlib.Path(sys.argv[1]).resolve(strict=True)
con = sqlite3.connect(p.as_uri() + '?mode=ro', uri=True, timeout=2)
con.execute('PRAGMA query_only=ON')
deadline = time.monotonic() + 10
con.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
con.execute('BEGIN')
rows = con.execute('SELECT version, applied_at FROM schema_migrations ORDER BY version').fetchall()
assert len(rows) <= 10000, 'STOP unexpected schema inventory size'
print(json.dumps(rows))
con.rollback()
con.close()
SQL_READ
```

Локально после этого сравнить полученные versions с approved checkout:

```bash
python3 - "$SCHEMA_OUTPUT" <<'COMPARE'
import json, pathlib, sys
shipped = {p.name for p in pathlib.Path('migrations').glob('*.sql')}
rows = json.load(open(sys.argv[1])); versions = [r[0] for r in rows]
assert len(set(versions)) == len(versions), 'STOP duplicate migration versions'
recorded = set(versions)
print(json.dumps({'not_recorded': sorted(shipped-recorded),
                  'unshipped_recorded': sorted(recorded-shipped)}, sort_keys=True))
assert not recorded-shipped, 'STOP unexpected server schema requires explicit compatibility decision'
COMPARE
```

`not_recorded` — кандидаты pending/deferred, не окончательный startup plan.
Проверить actual definitions/indexes и previous migration bundle, не подделывать
schema_migrations. Отсутствующая table/error/deadline — Unknown/STOP, не empty set.

## Связь с exact app release

CI `tools/build_operator_artifacts.sh` кладёт всю migrations directory в
migrations.tar.gz. Его SHA256 входит в build-manifest.migration_bundle.sha256 и
internal SHA256SUMS; весь app tar связан external .tar.gz.sha256.
`tools/verify_operator_artifact.py --enforce-workspace-bin-check` сравнивает
bundle membership и SQL hashes с approved checkout. Exact bundle checksum
возникнет после CI, заранее его не выдумывать: tar metadata влияет на archive bytes.
После upload ещё раз full checksum verification, затем installed SQL hashes
с этой таблицей и exact binary SHA. Installer меняет link; SQL применяется startup.

## Reuse127 и предел backward compatibility

Повтор доказанного upgrade не нужен: неизменны migrations, relevant production
inputs, fixtures и retained executables. Внешний evidence/MIGRATION_REUSE.json
содержит их hashes и привязку к accepted127 fresh-tests.json (новых запусков0).

- `tiny_budget_upgrade_has_no_activation_and_reopen_never_creates_money`:
  synthetic schema до0077/0078 → upgrade → absent experiment/reopen, no activation.
- `tiny_capital_upgrade_preserves_0077_active_history`: synthetic all migrations
  except0078, active0077 и NULL-fee reservation → upgrade → exact columns/history,
  held budget, existing identity; отказ переконвертировать её в native policy.

Эти tests используют accepted127 consumer и synthetic disposable DB. Они не
запускают старый production binary на новой schema. SQL readback также не old
binary proof. Actual server version/schema неизвестны: previous binary0078
compatibility не утверждается. Сейчас нет нового конкретного local compatibility
вопроса с установленным old consumer, поэтому targeted test/build0. До install
нужно actual previous version proof либо конкретное pretrade restore/recovery
решение. После новых financial records старую backup не накатывать.
