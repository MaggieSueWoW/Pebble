"""Verify Docker's actual context filtering with synthetic build inputs."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.skipif(
    os.getenv("PEBBLE_TEST_DOCKER_BUILDS") != "1",
    reason="Set PEBBLE_TEST_DOCKER_BUILDS=1 to enable isolated Docker context checks",
)
@pytest.mark.parametrize("web", [False, True], ids=["legacy", "web"])
def test_build_context_preserves_runtime_boundary(tmp_path, web):
    context = tmp_path / "context"
    context.mkdir()
    inputs = {
        "README.md",
        "pyproject.toml",
        "pebble/cli.py",
        "pebble/nested/helpers.py",
    }
    legacy_inputs = {"config-legacy.yaml", "service-account.json"}
    private_web_inputs = {
        "config-web.yaml",
        "config-web-admin.yaml",
        "config-web-preview.yaml",
    }
    excluded = {
        ".env",
        ".env.local",
        ".pebble-service-account.json",
        "deploy/private/config.yaml",
        "apps/web/dist/index.html",
        "pebble/__pycache__/cli.pyc",
        "pebble/private.json",
    }
    for name in inputs | legacy_inputs | private_web_inputs | excluded:
        path = context / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("synthetic build fixture\n")
    shutil.copyfile(ROOT / ".dockerignore", context / ".dockerignore")
    dockerfile = "Dockerfile.web" if web else "Dockerfile"
    if web:
        shutil.copyfile(
            ROOT / "Dockerfile.web.dockerignore",
            context / "Dockerfile.web.dockerignore",
        )
    # A scratch export tests the real ignore rules without installing packages,
    # loading real credentials, or creating/replacing a running container.
    (context / dockerfile).write_text("FROM scratch\nCOPY . /\n")
    output = tmp_path / "output"
    subprocess.run(
        [
            "docker",
            "build",
            "--output",
            f"type=local,dest={output}",
            "-f",
            str(context / dockerfile),
            str(context),
        ],
        env={**os.environ, "DOCKER_BUILDKIT": "1"},
        capture_output=True,
        text=True,
        check=True,
        timeout=90,
    )
    for name in inputs | (set() if web else legacy_inputs):
        assert (output / name).is_file(), f"Required build input excluded: {name}"
    leaked = {
        name
        for name in excluded | private_web_inputs | (legacy_inputs if web else set())
        if (output / name).exists()
    }
    assert not leaked, f"Unexpected build inputs included: {sorted(leaked)}"
