import gc
import time
from contextlib import contextmanager
from unittest.mock import patch

import objgraph
import pytest
from dagster import AssetKey, AssetMaterialization, EventLogEntry
from dagster._core.events import (
    AssetWipedData,
    DagsterEvent,
    DagsterEventType,
    StepMaterializationData,
)
from dagster._core.instance import RUNLESS_JOB_NAME, RUNLESS_RUN_ID
from dagster._core.storage.event_log.base import EventLogCursor
from dagster._core.storage.event_log.migration import ASSET_KEY_INDEX_COLS
from dagster._core.storage.event_log.schema import SecondaryIndexMigrationTable
from dagster._core.test_utils import ensure_dagster_tests_import, instance_for_test
from dagster._core.utils import make_new_run_id
from dagster_postgres.event_log import PostgresEventLogStorage
from dagster_shared.yaml_utils import safe_load_yaml
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.pool import QueuePool

ensure_dagster_tests_import()
from dagster_tests.storage_tests.utils.event_log_storage import (
    TestEventLogStorage,
    create_test_event_log_record,
)


@contextmanager
def _clean_storage(conn_string):
    storage = PostgresEventLogStorage.create_clean_storage(conn_string)
    assert storage
    try:
        yield storage
    finally:
        storage.dispose()


class TestPostgresEventLogStorage(TestEventLogStorage):
    __test__ = True

    @pytest.fixture(name="instance", scope="function")
    def instance(self, conn_string):
        PostgresEventLogStorage.create_clean_storage(conn_string)

        with instance_for_test(
            overrides={"storage": {"postgres": {"postgres_url": conn_string}}}
        ) as instance:
            yield instance

    @pytest.fixture(scope="function", name="storage")
    def event_log_storage(self, instance):
        event_log_storage = instance.event_log_storage
        assert isinstance(event_log_storage, PostgresEventLogStorage)
        yield event_log_storage

    def can_wipe_asset_partitions(self) -> bool:
        return False

    def test_event_log_storage_two_watchers(self, conn_string):
        with _clean_storage(conn_string) as storage:
            run_id = make_new_run_id()
            watched_1 = []
            watched_2 = []

            def watch_one(event, _cursor):
                watched_1.append(event)

            def watch_two(event, _cursor):
                watched_2.append(event)

            assert len(storage.get_logs_for_run(run_id)) == 0

            storage.store_event(create_test_event_log_record(str(1), run_id=run_id))
            assert len(storage.get_logs_for_run(run_id)) == 1
            assert len(watched_1) == 0

            storage.watch(run_id, str(EventLogCursor.from_storage_id(1)), watch_one)

            storage.store_event(create_test_event_log_record(str(2), run_id=run_id))
            storage.store_event(create_test_event_log_record(str(3), run_id=run_id))

            storage.watch(run_id, str(EventLogCursor.from_storage_id(3)), watch_two)
            storage.store_event(create_test_event_log_record(str(4), run_id=run_id))

            attempts = 10
            while (len(watched_1) < 3 or len(watched_2) < 1) and attempts > 0:
                time.sleep(0.5)
                attempts -= 1
            assert len(watched_1) == 3
            assert len(watched_2) == 1

            assert len(storage.get_logs_for_run(run_id)) == 4

            storage.end_watch(run_id, watch_one)
            time.sleep(0.3)  # this value scientifically selected from a range of attractive values
            storage.store_event(create_test_event_log_record(str(5), run_id=run_id))

            attempts = 10
            while len(watched_2) < 2 and attempts > 0:
                time.sleep(0.5)
                attempts -= 1
            assert len(watched_1) == 3
            assert len(watched_2) == 2

            storage.end_watch(run_id, watch_two)

            assert len(storage.get_logs_for_run(run_id)) == 5

            storage.delete_events(run_id)

            assert len(storage.get_logs_for_run(run_id)) == 0
            assert len(watched_1) == 3
            assert len(watched_2) == 2

            assert [int(evt.message) for evt in watched_1] == [2, 3, 4]
            assert [int(evt.message) for evt in watched_2] == [4, 5]
            assert len(objgraph.by_type("SqlPollingEventWatcher")) == 1

        # ensure we clean up poller on exit
        gc.collect()
        assert len(objgraph.by_type("SqlPollingEventWatcher")) == 0

    def test_load_from_config(self, hostname):
        url_cfg = f"""
        event_log_storage:
            module: dagster_postgres.event_log
            class: PostgresEventLogStorage
            config:
                postgres_url: postgresql://test:test@{hostname}:5432/test
        """

        explicit_cfg = f"""
        event_log_storage:
            module: dagster_postgres.event_log
            class: PostgresEventLogStorage
            config:
                postgres_db:
                    username: test
                    password: test
                    hostname: {hostname}
                    db_name: test
        """

        with instance_for_test(overrides=safe_load_yaml(url_cfg)) as from_url_instance:
            from_url = from_url_instance._event_storage  # noqa: SLF001

            with instance_for_test(overrides=safe_load_yaml(explicit_cfg)) as explicit_instance:
                from_explicit = explicit_instance._event_storage  # noqa: SLF001

                assert from_url.postgres_url == from_explicit.postgres_url  # ty: ignore[unresolved-attribute]


def test_has_table_returns_connection_to_pool(conn_string):
    # Mirrors the webserver's pooling, where a connection left checked out by has_table()
    # starves every other event log query until cyclic GC happens to reclaim it.
    with _clean_storage(conn_string) as storage:
        storage.optimize_for_webserver(statement_timeout=5000, pool_recycle=3600, max_overflow=0)
        engine = storage._engine  # noqa: SLF001
        pool = engine.pool
        assert isinstance(pool, QueuePool)
        assert pool.size() == 1

        # Retain every Connection so refcounting cannot return one to the pool and hide a leak.
        retained_connections: list[Connection] = []
        engine_connect = engine.connect

        def connect_and_retain() -> Connection:
            connection = engine_connect()
            retained_connections.append(connection)
            return connection

        try:
            with patch.object(engine, "connect", side_effect=connect_and_retain):
                for _ in range(2):
                    assert storage.has_table("event_logs")
                    assert pool.checkedout() == 0
        finally:
            for connection in retained_connections:
                connection.close()


def test_all_asset_keys_excludes_wiped_asset_on_legacy_index_path(conn_string):
    # Regression test for the legacy (pre-ASSET_KEY_INDEX_COLS-migration) read path, where wiped
    # assets are filtered by comparing the wipe timestamp against the latest event log timestamp.
    # The ASSET_WIPED event stored immediately after a wipe (and any other event type that does
    # not update last_materialization_timestamp on the migrated path) must not make the wiped
    # asset appear live.
    asset_key = AssetKey(["asset_one"])
    with _clean_storage(conn_string) as storage:
        # simulate a storage that has not run the ASSET_KEY_INDEX_COLS data migration
        with storage.index_connection() as conn:
            conn.execute(SecondaryIndexMigrationTable.delete())
        storage._secondary_index_cache.clear()  # noqa: SLF001
        assert not storage.has_secondary_index(ASSET_KEY_INDEX_COLS)

        storage.store_event(
            EventLogEntry(
                error_info=None,
                level="debug",
                user_message="",
                run_id=make_new_run_id(),
                timestamp=time.time(),
                dagster_event=DagsterEvent(
                    DagsterEventType.ASSET_MATERIALIZATION.value,
                    "nonce",
                    event_specific_data=StepMaterializationData(
                        AssetMaterialization(asset_key=asset_key)
                    ),
                ),
            )
        )
        assert asset_key in storage.all_asset_keys()

        storage.wipe_asset(asset_key)
        storage.store_event(
            EventLogEntry(
                error_info=None,
                level="debug",
                user_message="",
                run_id=RUNLESS_RUN_ID,
                timestamp=time.time(),
                dagster_event=DagsterEvent(
                    event_type_value=DagsterEventType.ASSET_WIPED.value,
                    job_name=RUNLESS_JOB_NAME,
                    event_specific_data=AssetWipedData(asset_key=asset_key, partition_keys=None),
                ),
            )
        )

        assert asset_key not in storage.all_asset_keys()
        assert storage.get_latest_materialization_events([asset_key]).get(asset_key) is None


@contextmanager
def _queued_cancellation_instance(conn_string, use_default_launcher=False):
    overrides = {"storage": {"postgres": {"postgres_url": conn_string}}}
    if use_default_launcher:
        overrides["run_launcher"] = {
            "module": "dagster._core.launcher.default_run_launcher",
            "class": "DefaultRunLauncher",
        }
    with (
        _clean_storage(conn_string),
        instance_for_test(overrides=overrides) as instance,
    ):
        instance.wipe()
        yield instance


def _create_queued_run(instance):
    from dagster._core.remote_origin import (
        GrpcServerCodeLocationOrigin,
        RemoteJobOrigin,
        RemoteRepositoryOrigin,
    )
    from dagster._core.storage.dagster_run import DagsterRunStatus
    from dagster._core.test_utils import create_run_for_test

    return create_run_for_test(
        instance,
        status=DagsterRunStatus.QUEUED,
        remote_job_origin=RemoteJobOrigin(
            RemoteRepositoryOrigin(GrpcServerCodeLocationOrigin("localhost", port=1234), "repo"),
            "job",
        ),
    )


def test_run_cancellation_batch(conn_string):
    from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatus
    from dagster._core.storage.runs.schema import RunsTable
    from dagster._serdes import deserialize_value
    from sqlalchemy import event as sql_event

    with _queued_cancellation_instance(conn_string) as instance:
        runs = [_create_queued_run(instance) for _ in range(3)]
        statements = []

        def before_execute(conn, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        sql_event.listen(Engine, "before_cursor_execute", before_execute)
        try:
            instance.report_run_cancellation_batch(runs)
        finally:
            sql_event.remove(Engine, "before_cursor_execute", before_execute)
        assert len([s for s in statements if s.startswith("INSERT INTO event_logs")]) == 1
        assert len([s for s in statements if "pg_notify" in s]) == 1
        assert len([s for s in statements if s.startswith("UPDATE runs")]) == 1
        for run in runs:
            record = instance.get_run_record_by_id(run.run_id)
            assert record.dagster_run.status == DagsterRunStatus.CANCELED
            assert record.end_time is not None
            logs = instance.all_logs(run.run_id)
            assert [e.dagster_event_type for e in logs] == [
                DagsterEventType.RUN_CANCELING,
                DagsterEventType.RUN_CANCELED,
            ]
            assert record.end_time == logs[-1].timestamp
        with instance.run_storage.connect() as conn:
            for (body,) in conn.execute(RunsTable.select().with_only_columns(RunsTable.c.run_body)):
                assert deserialize_value(body, DagsterRun).status == DagsterRunStatus.CANCELED


@pytest.mark.parametrize("cancel_queued", [False, True])
@pytest.mark.parametrize("failure_point", ["event_insert", "notification", "later_chunk"])
def test_atomic_cancellation_rolls_back_all_state(conn_string, failure_point, cancel_queued):
    from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter
    from dagster._core.storage.tags import RUN_TERMINATION_REQUESTED_TAG
    from sqlalchemy import event as sql_event

    with _queued_cancellation_instance(conn_string) as instance:
        create_run = _create_queued_run if cancel_queued else _create_started_run
        report = (
            instance.report_run_cancellation_batch
            if cancel_queued
            else instance.report_run_canceling_batch
        )
        original_status = DagsterRunStatus.QUEUED if cancel_queued else DagsterRunStatus.STARTED
        target_status = DagsterRunStatus.CANCELED if cancel_queued else DagsterRunStatus.CANCELING
        runs = [create_run(instance) for _ in range(501 if failure_point == "later_chunk" else 2)]
        inserts = 0

        def fail(conn, cursor, statement, parameters, context, executemany):
            nonlocal inserts
            if statement.startswith("INSERT INTO event_logs"):
                inserts += 1
                if failure_point == "event_insert" or (
                    failure_point == "later_chunk" and inserts == 2
                ):
                    raise RuntimeError("injected transaction failure")
            if "pg_notify" in statement and failure_point == "notification":
                raise RuntimeError("injected transaction failure")

        sql_event.listen(Engine, "before_cursor_execute", fail)
        try:
            with pytest.raises(RuntimeError, match="injected transaction failure"):
                report(runs)
        finally:
            sql_event.remove(Engine, "before_cursor_execute", fail)
        assert all(run.status == original_status for run in instance.get_runs())
        assert not instance.get_run_ids(RunsFilter(tags={RUN_TERMINATION_REQUESTED_TAG: "true"}))
        assert not instance.all_logs(runs[0].run_id)
        assert not instance.all_logs(runs[-1].run_id)
        report(runs)
        assert all(run.status == target_status for run in instance.get_runs())


def test_atomic_cancellation_revalidates_status(conn_string):
    from dagster._core.storage.dagster_run import DagsterRunStatus

    with _queued_cancellation_instance(conn_string) as instance:
        run = _create_queued_run(instance)
        instance.report_dagster_event(
            DagsterEvent(DagsterEventType.RUN_START.value, job_name=run.job_name), run.run_id
        )
        assert instance.event_log_storage.cancel_queued_runs([run]) == []
        assert instance.get_run_by_id(run.run_id).status == DagsterRunStatus.STARTED
        assert [e.dagster_event_type for e in instance.all_logs(run.run_id)] == [
            DagsterEventType.RUN_START
        ]


def test_postgres_listener_uses_serial_reporting(conn_string):
    from dagster._core.storage.dagster_run import DagsterRunStatus

    with _queued_cancellation_instance(conn_string) as instance:
        run = _create_queued_run(instance)
        seen = []

        def listener(event):
            seen.append(
                (
                    event.dagster_event_type,
                    instance.get_run_by_id(run.run_id).status,
                    len(instance.all_logs(run.run_id)),
                )
            )

        instance.add_event_listener(run.run_id, listener)
        with patch.object(instance.event_log_storage, "cancel_queued_runs") as batch:
            instance.report_run_cancellation_batch([run])
            batch.assert_not_called()
        assert seen == [
            (DagsterEventType.RUN_CANCELING, DagsterRunStatus.CANCELING, 1),
            (DagsterEventType.RUN_CANCELED, DagsterRunStatus.CANCELED, 2),
        ]


def test_queued_state_and_events_are_not_visible_before_commit(conn_string):
    from dagster._core.storage.dagster_run import DagsterRunStatus
    from dagster._core.storage.event_log.schema import SqlEventLogStorageTable
    from dagster._core.storage.runs.schema import RunsTable
    from sqlalchemy import (
        create_engine,
        event as sql_event,
        func,
        select,
    )

    with _queued_cancellation_instance(conn_string) as instance:
        run = _create_queued_run(instance)
        observed = []
        engine = create_engine(conn_string, isolation_level="AUTOCOMMIT")

        def observe(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith("INSERT INTO event_logs"):
                with engine.connect() as other:
                    status = other.execute(
                        select(RunsTable.c.status).where(RunsTable.c.run_id == run.run_id)
                    ).scalar()
                    count = other.execute(
                        select(func.count()).select_from(SqlEventLogStorageTable)
                    ).scalar()
                    observed.append((status, count))

        sql_event.listen(Engine, "before_cursor_execute", observe)
        try:
            instance.report_run_cancellation_batch([run])
        finally:
            sql_event.remove(Engine, "before_cursor_execute", observe)
            engine.dispose()
        assert observed == [(DagsterRunStatus.QUEUED.value, 0)]
        assert instance.get_run_by_id(run.run_id).status == DagsterRunStatus.CANCELED


def test_postgres_custom_run_storage_hook_uses_serial_path(conn_string):
    with _queued_cancellation_instance(conn_string) as instance:
        run = _create_queued_run(instance)
        with (
            patch.object(
                instance.run_storage,
                "handle_run_event",
                wraps=instance.run_storage.handle_run_event,
            ) as hook,
            patch.object(instance.event_log_storage, "cancel_queued_runs") as batch,
        ):
            instance.report_run_cancellation_batch([run])
            assert hook.call_count == 2
            batch.assert_not_called()


def test_postgres_mixed_storage_does_not_enable_atomic_fast_path(conn_string):
    with (
        _clean_storage(conn_string),
        instance_for_test(
            overrides={
                "event_log_storage": {
                    "module": "dagster_postgres.event_log",
                    "class": "PostgresEventLogStorage",
                    "config": {"postgres_url": conn_string},
                }
            }
        ) as instance,
    ):
        assert not instance.event_log_storage.supports_queued_run_cancellation
        run = _create_queued_run(instance)
        instance.report_run_cancellation_batch([run])
        assert len(instance.all_logs(run.run_id)) == 2


def _create_started_run(instance):
    from dagster._core.storage.dagster_run import DagsterRunStatus
    from dagster._core.storage.tags import BACKFILL_ID_TAG
    from dagster._core.test_utils import create_run_for_test

    return create_run_for_test(
        instance, status=DagsterRunStatus.STARTED, tags={BACKFILL_ID_TAG: "backfill"}
    )


@contextmanager
def _started_cancellation_instance(conn_string, num_runs=3):
    import json

    from dagster._core.storage.tags import GRPC_INFO_TAG
    from dagster_tests.launcher_tests.test_batch_termination import cancellation_server

    with _queued_cancellation_instance(conn_string, use_default_launcher=True) as instance:
        runs = [_create_started_run(instance) for _ in range(num_runs)]
        with cancellation_server([run.run_id for run in runs]) as (server, client):
            for run in runs:
                instance.add_run_tags(
                    run.run_id,
                    {GRPC_INFO_TAG: json.dumps({"host": client.host, "port": client.port})},
                )
            yield instance, [instance.get_run_by_id(run.run_id) for run in runs], server


def test_started_cancellation_batches_sql_and_preserves_running_state(conn_string):
    from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatus, RunsFilter
    from dagster._core.storage.runs.schema import RunsTable
    from dagster._core.storage.tags import RUN_TERMINATION_REQUESTED_TAG
    from dagster._serdes import deserialize_value
    from sqlalchemy import event as sql_event

    with _queued_cancellation_instance(conn_string) as instance:
        runs = [_create_started_run(instance) for _ in range(3)]
        statements = []

        def observe(conn, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        sql_event.listen(Engine, "before_cursor_execute", observe)
        try:
            marked = instance.report_run_canceling_batch(runs)
        finally:
            sql_event.remove(Engine, "before_cursor_execute", observe)
        assert {run.run_id for run in marked} == {run.run_id for run in runs}
        for prefix in ["UPDATE runs", "INSERT INTO event_logs", "INSERT INTO run_tags"]:
            assert len([statement for statement in statements if statement.startswith(prefix)]) == 1
        assert len([statement for statement in statements if "pg_notify" in statement]) == 1
        assert set(
            instance.get_run_ids(RunsFilter(tags={RUN_TERMINATION_REQUESTED_TAG: "true"}))
        ) == {run.run_id for run in runs}
        for run in runs:
            record = instance.get_run_record_by_id(run.run_id)
            assert record.dagster_run.status == DagsterRunStatus.CANCELING
            assert record.end_time is None
            assert record.dagster_run.tags[RUN_TERMINATION_REQUESTED_TAG] == "true"
            assert [event.dagster_event_type for event in instance.all_logs(run.run_id)] == [
                DagsterEventType.RUN_CANCELING
            ]
        # Repeated requests reuse the persisted intent.
        assert len(instance.report_run_canceling_batch(runs)) == 3
        for run in runs:
            assert len(instance.all_logs(run.run_id)) == 1
        with instance.run_storage.connect() as conn:
            for (body,) in conn.execute(RunsTable.select().with_only_columns(RunsTable.c.run_body)):
                persisted = deserialize_value(body, DagsterRun)
                assert persisted.status == DagsterRunStatus.CANCELING
                assert persisted.tags[RUN_TERMINATION_REQUESTED_TAG] == "true"


def test_started_cancellation_revalidates_terminal_status_and_route(conn_string):
    from dagster._core.storage.tags import GRPC_INFO_TAG

    with _queued_cancellation_instance(conn_string) as instance:
        finished, moved, active = [_create_started_run(instance) for _ in range(3)]
        instance.report_dagster_event(
            DagsterEvent(DagsterEventType.RUN_SUCCESS.value, job_name=finished.job_name),
            finished.run_id,
        )
        instance.add_run_tags(moved.run_id, {GRPC_INFO_TAG: "changed"})
        assert [
            run.run_id for run in instance.report_run_canceling_batch([finished, moved, active])
        ] == [active.run_id]
        assert not instance.all_logs(moved.run_id)
        assert [event.dagster_event_type for event in instance.all_logs(finished.run_id)] == [
            DagsterEventType.RUN_SUCCESS
        ]


@pytest.mark.parametrize("failure", ["before_rpc", "lost_rpc_reply", "lost_commit_reply"])
def test_started_cancellation_recovers_durable_intents(conn_string, failure):
    import logging

    from dagster._core.errors import DagsterUserCodeUnreachableError
    from dagster._core.execution.backfill import cancel_backfill_runs_and_cancellation_complete
    from dagster._core.storage.dagster_run import DagsterRunStatus, RunsFilter
    from dagster._core.storage.tags import RUN_TERMINATION_REQUESTED_TAG
    from dagster._grpc.client import DagsterGrpcClient

    with _started_cancellation_instance(conn_string) as (instance, runs, server):
        launcher = instance.run_launcher
        original = DagsterGrpcClient.cancel_executions

        def fail_rpc(self, run_ids, **kwargs):
            if failure == "lost_rpc_reply":
                original(self, run_ids, **kwargs)
            raise DagsterUserCodeUnreachableError("injected lost RPC")

        def fail_commit(runs):
            instance.event_log_storage.mark_runs_canceling(runs)
            raise RuntimeError("injected lost commit reply")

        if failure == "lost_commit_reply":
            with patch.object(instance, "report_run_canceling_batch", side_effect=fail_commit):
                with pytest.raises(RuntimeError, match="lost commit reply"):
                    launcher.terminate_runs([run.run_id for run in runs])
        else:
            with patch.object(DagsterGrpcClient, "cancel_executions", fail_rpc):
                with pytest.raises(DagsterUserCodeUnreachableError, match="lost RPC"):
                    launcher.terminate_runs([run.run_id for run in runs])
        assert (
            len(
                instance.get_runs(
                    RunsFilter(
                        statuses=[DagsterRunStatus.CANCELING],
                        tags={RUN_TERMINATION_REQUESTED_TAG: "true"},
                    )
                )
            )
            == 3
        )
        deadlines = dict(server._termination_times)  # noqa: SLF001
        assert not cancel_backfill_runs_and_cancellation_complete(
            instance, "backfill", logging.getLogger(__name__)
        )
        assert all(flag.is_set() for flag in server._termination_events.values())  # noqa: SLF001
        assert all(
            server._termination_times[run_id] == deadline  # noqa: SLF001
            for run_id, deadline in deadlines.items()
        )
        assert all(len(instance.all_logs(run.run_id)) == 1 for run in runs)
        for run in runs:
            instance.report_run_canceled(instance.get_run_by_id(run.run_id))
        assert cancel_backfill_runs_and_cancellation_complete(
            instance, "backfill", logging.getLogger(__name__)
        )


def test_concurrent_started_cancellation_does_not_duplicate_intents(conn_string):
    from concurrent.futures import ThreadPoolExecutor

    from dagster._core.storage.runs.schema import RunTagsTable
    from dagster._core.storage.tags import RUN_TERMINATION_REQUESTED_TAG

    with _queued_cancellation_instance(conn_string) as instance:
        runs = [_create_started_run(instance) for _ in range(3)]
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(instance.report_run_canceling_batch, runs) for _ in range(2)]
            assert all(len(future.result()) == len(runs) for future in futures)
        assert all(len(instance.all_logs(run.run_id)) == 1 for run in runs)
        with instance.run_storage.connect() as conn:
            rows = conn.execute(
                RunTagsTable.select().where(RunTagsTable.c.key == RUN_TERMINATION_REQUESTED_TAG)
            ).fetchall()
        assert len(rows) == len(runs)


def test_started_listener_still_observes_serial_reporting(conn_string):
    from dagster._core.storage.dagster_run import DagsterRunStatus

    with _started_cancellation_instance(conn_string, num_runs=1) as (instance, runs, _):
        run = runs[0]
        seen = []
        instance.add_event_listener(
            run.run_id,
            lambda event: seen.append(
                (event.dagster_event_type, instance.get_run_by_id(run.run_id).status)
            ),
        )
        with patch.object(instance.event_log_storage, "mark_runs_canceling") as batch:
            assert instance.run_launcher.terminate_runs([run.run_id]) == {run.run_id: True}
            batch.assert_not_called()
        assert seen == [(DagsterEventType.RUN_CANCELING, DagsterRunStatus.CANCELING)]


def test_started_intent_survives_legacy_server_and_stale_run_body(conn_string):
    import grpc
    from dagster._core.storage.dagster_run import DagsterRunStatus
    from dagster._core.storage.runs.schema import RunsTable
    from dagster._core.storage.tags import RUN_TERMINATION_REQUESTED_TAG
    from dagster._grpc.server import DagsterApiServer
    from dagster._serdes import serialize_value
    from dagster_tests.launcher_tests.test_batch_termination import cancellation_server

    with _queued_cancellation_instance(conn_string, use_default_launcher=True) as instance:
        run = _create_started_run(instance)
        instance.report_run_canceling_batch([run])
        # A concurrent tag writer can replace the serialized run body.
        with instance.run_storage.connect() as conn:
            conn.execute(
                RunsTable.update()
                .where(RunsTable.c.run_id == run.run_id)
                .values(run_body=serialize_value(run.with_status(DagsterRunStatus.CANCELING)))
            )
        assert RUN_TERMINATION_REQUESTED_TAG not in instance.get_run_by_id(run.run_id).tags

        def unimplemented(request, context):
            context.abort(grpc.StatusCode.UNIMPLEMENTED, "old server")

        with (
            patch.object(DagsterApiServer, "CancelExecutions", side_effect=unimplemented),
            cancellation_server([run.run_id]) as (legacy, legacy_client),
        ):
            import json

            from dagster._core.storage.tags import GRPC_INFO_TAG

            instance.add_run_tags(
                run.run_id,
                {
                    GRPC_INFO_TAG: json.dumps(
                        {"host": legacy_client.host, "port": legacy_client.port}
                    )
                },
            )
            assert instance.run_launcher.terminate_runs([run.run_id])[run.run_id]
            deadline = legacy._termination_times[run.run_id]  # noqa: SLF001
            instance.run_launcher.terminate_runs([run.run_id])
            assert legacy._termination_times[run.run_id] == deadline  # noqa: SLF001
        assert len(instance.all_logs(run.run_id)) == 1
