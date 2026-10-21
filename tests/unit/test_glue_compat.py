import pytest

from pipelines.jobs.glue_compat import ON_GLUE, MissingJobArgumentError, parse_local_args


def test_not_on_glue_locally() -> None:
    assert ON_GLUE is False


def test_parses_glue_style_args_and_ignores_unknown() -> None:
    argv = ["--JOB_NAME", "payments_etl", "--ds", "2026-10-20", "--job-bookmark-option", "x"]
    args = parse_local_args(argv, ["JOB_NAME", "ds"], ["lake_root"])
    assert args == {"JOB_NAME": "payments_etl", "ds": "2026-10-20"}


def test_optional_args_are_picked_up() -> None:
    argv = ["--JOB_NAME", "j", "--ds", "d", "--lake_root", "s3://b/lake"]
    assert parse_local_args(argv, ["JOB_NAME", "ds"], ["lake_root"])["lake_root"] == "s3://b/lake"


def test_missing_required_arg_raises_like_glue() -> None:
    with pytest.raises(MissingJobArgumentError, match="ds"):
        parse_local_args(["--JOB_NAME", "j"], ["JOB_NAME", "ds"], [])
