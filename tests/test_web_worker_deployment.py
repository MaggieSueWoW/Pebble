"""Exercise deployment failure ordering without touching a Docker daemon."""

import json
import os
import subprocess
from pathlib import Path

import pytest

REVISION = "a" * 40
SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "web-worker.sh"


@pytest.fixture
def deployment(tmp_path):
    docker = tmp_path / "docker"
    docker.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, sys\n"
        "args = sys.argv[1:]\n"
        "with open(os.environ['DOCKER_CALLS'], 'a') as log:\n"
        "    log.write(json.dumps(args) + '\\n')\n"
        "failure = os.environ.get('FAIL_STEP')\n"
        "if args[:2] == ['image', 'inspect']:\n"
        "    if failure == 'missing-image': sys.exit(1)\n"
        "    print(os.environ['IMAGE_METADATA'])\n"
        "elif args[-2:] == ['config', '--images']:\n"
        "    if failure == 'bad-config': sys.exit(1)\n"
        "    print(os.environ['SELECTED_IMAGE'])\n"
        "elif 'run' in args and failure == 'runtime':\n"
        "    sys.exit(1)\n"
    )
    docker.chmod(0o755)
    log = tmp_path / "calls.jsonl"
    env = {
        **os.environ,
        "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}",
        "DOCKER_CALLS": str(log),
        "SELECTED_IMAGE": f"pebble-web:{REVISION}",
        "IMAGE_METADATA": f"{REVISION} linux/amd64",
        "PEBBLE_WEB_SINGLETON_CONFIRMED": "yes",
    }

    def run(**changes):
        result = subprocess.run(
            ["bash", str(SCRIPT), "unused.env", "update"],
            env={**env, **changes},
            capture_output=True,
            text=True,
        )
        calls = (
            [json.loads(line) for line in log.read_text().splitlines()]
            if log.exists()
            else []
        )
        return result, calls

    return run


@pytest.mark.parametrize("failure", ["bad-config", "missing-image", "runtime"])
def test_failed_preflight_does_not_stop_worker(deployment, failure):
    result, calls = deployment(FAIL_STEP=failure)
    assert result.returncode != 0
    assert not any("stop" in call or "up" in call for call in calls)


@pytest.mark.parametrize(
    "changes",
    [
        {"SELECTED_IMAGE": "pebble-web:latest"},
        {"IMAGE_METADATA": f"{'b' * 40} linux/amd64"},
        {"IMAGE_METADATA": f"{REVISION} linux/arm64"},
    ],
)
def test_wrong_release_does_not_run_or_stop_worker(deployment, changes):
    result, calls = deployment(**changes)
    assert result.returncode != 0
    assert not any("run" in call or "stop" in call or "up" in call for call in calls)


def test_update_checks_replacement_before_stop_and_start(deployment):
    result, calls = deployment()
    assert result.returncode == 0, result.stderr
    operations = [
        next(op for op in ("config", "inspect", "run", "stop", "up") if op in call)
        for call in calls
    ]
    assert operations == ["config", "inspect", "run", "stop", "up"]
    assert "--pull" in calls[2] and "never" in calls[2]
    for call in calls:
        if call[0] == "compose":
            assert call[1:3] == ["--project-name", "pebble-web"]
            assert call[call.index("-f") + 1] == "docker-compose.web.yaml"
    assert calls[-1][-2:] == ["never", "web-worker"]


def test_update_requires_singleton_acknowledgement(deployment):
    result, calls = deployment(PEBBLE_WEB_SINGLETON_CONFIRMED="")
    assert result.returncode != 0
    assert calls == []
