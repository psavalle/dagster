import logging
from unittest.mock import patch

import pytest
from dagster._core.execution.backfill import cancel_backfill_runs_and_cancellation_complete
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster._core.storage.tags import BACKFILL_ID_TAG
from dagster._core.test_utils import create_run_for_test, instance_for_test


def _started_run(instance):
    return create_run_for_test(
        instance, status=DagsterRunStatus.STARTED, tags={BACKFILL_ID_TAG: "backfill"}
    )


def test_failed_page_does_not_block_later_pages():
    with instance_for_test() as instance:
        failed, healthy = [_started_run(instance) for _ in range(2)]

        def terminate(run_ids):
            if run_ids == [failed.run_id]:
                raise RuntimeError("server unavailable")
            assert run_ids == [healthy.run_id]
            instance.report_run_canceling(instance.get_run_by_id(healthy.run_id))
            return {healthy.run_id: True}

        with (
            patch("dagster._core.execution.backfill.CANCELABLE_RUNS_BATCH_SIZE", 1),
            patch.object(instance.run_launcher, "terminate_runs", side_effect=terminate),
        ):
            with pytest.raises(RuntimeError, match="server unavailable"):
                cancel_backfill_runs_and_cancellation_complete(
                    instance, "backfill", logging.getLogger(__name__)
                )
        assert instance.get_run_by_id(failed.run_id).status == DagsterRunStatus.STARTED
        assert instance.get_run_by_id(healthy.run_id).status == DagsterRunStatus.CANCELING


def test_non_mutating_launcher_is_attempted_once_per_pass():
    with instance_for_test() as instance:
        run = _started_run(instance)
        with patch.object(
            instance.run_launcher, "terminate_runs", return_value={run.run_id: False}
        ) as terminate:
            assert not cancel_backfill_runs_and_cancellation_complete(
                instance, "backfill", logging.getLogger(__name__)
            )
            terminate.assert_called_once_with([run.run_id])
        assert instance.get_run_by_id(run.run_id).status == DagsterRunStatus.STARTED
