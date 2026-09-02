"""Fixtures for integration tests that talk to a real Pathling FHIR server.

Spawns a session-scoped Pathling container (publicly available
`ghcr.io/aehrc/pathling`) via the Docker CLI and yields its base URL.

By default the image tag is resolved at session start from the upstream
`aehrc/pathling` GitHub release feed — the newest `server-v*` tag wins — so
the suite tracks new server releases without code changes. Override knobs:

* ``PATHLING_IMAGE`` — full image reference, e.g.
  ``ghcr.io/aehrc/pathling:2.0.0`` or ``my.registry/pathling:custom``.
  Wins outright; no upstream lookup is performed.
* ``PATHLING_VERSION`` — version only, e.g. ``2.0.0``. Combined with
  ``PATHLING_REPOSITORY`` (default ``ghcr.io/aehrc/pathling``).
* ``PATHLING_REPOSITORY`` — image repository without tag. Defaults to
  ``ghcr.io/aehrc/pathling``.
* ``PATHLING_FALLBACK_VERSION`` — used if the upstream GitHub API call
  fails (rate limit, offline run, etc.). Defaults to the last version
  this suite was known to pass against.

The tests are gated behind the ``integration`` pytest marker. Run them with::

    pytest -m integration

Tests are skipped automatically if Docker is not installed or not running.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request

import pytest

DEFAULT_REPOSITORY = "ghcr.io/aehrc/pathling"
# Last-known-good fallback used when the GitHub release feed is unreachable.
# Bump this opportunistically when the scheduled compat job picks up a new
# version successfully.
FALLBACK_VERSION = "3.0.0"
STARTUP_TIMEOUT_SECONDS = 180
GITHUB_RELEASES_URL = "https://api.github.com/repos/aehrc/pathling/releases?per_page=50"


def _docker_available() -> bool:
    if not shutil.which("docker"):
        return False
    try:
        subprocess.run(
            ["docker", "info"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
        return False
    return True


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _latest_server_release() -> str | None:
    """Return the newest ``server-v*`` tag from aehrc/pathling, or None on error.

    Mirrors the resolution logic in ``.github/workflows/pathling-compat.yml``
    so local runs and CI converge on the same image.
    """
    try:
        req = urllib.request.Request(
            GITHUB_RELEASES_URL,
            headers={"Accept": "application/vnd.github+json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            releases = json.load(resp)
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, json.JSONDecodeError):
        return None

    server_releases = [
        r for r in releases
        if not r.get("prerelease")
        and isinstance(r.get("tag_name"), str)
        and r["tag_name"].startswith("server-v")
    ]
    if not server_releases:
        return None

    def _version_key(release: dict) -> tuple[int, ...]:
        raw = release["tag_name"].removeprefix("server-v")
        parts = raw.split(".")
        try:
            return tuple(int(p) for p in parts)
        except ValueError:
            return (0,)  # demote unparseable tags to the bottom

    server_releases.sort(key=_version_key)
    return server_releases[-1]["tag_name"].removeprefix("server-v")


def _resolve_image() -> str:
    """Pick a Pathling image reference per the env-var precedence rules."""
    explicit = os.environ.get("PATHLING_IMAGE")
    if explicit:
        return explicit

    repo = os.environ.get("PATHLING_REPOSITORY", DEFAULT_REPOSITORY)
    version = os.environ.get("PATHLING_VERSION")
    if not version:
        version = _latest_server_release()
    if not version:
        version = os.environ.get("PATHLING_FALLBACK_VERSION", FALLBACK_VERSION)
    return f"{repo}:{version}"


def _wait_for_metadata(base_url: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{base_url}/metadata", timeout=5) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as e:
            last_err = e
        time.sleep(2)
    raise RuntimeError(
        f"Pathling at {base_url} did not become ready in {timeout}s; last error: {last_err}"
    )


@pytest.fixture(scope="session")
def pathling_base_url() -> str:
    """Spin up a Pathling container and return the FHIR base URL.

    Skips the test if Docker is not available. The container is torn down at
    session end.
    """
    if not _docker_available():
        pytest.skip("Docker is not available — integration tests skipped")

    image = _resolve_image()
    print(f"[integration] using Pathling image: {image}")
    port = _pick_free_port()
    container_name = f"sqlonfhir-it-{port}"

    # Best-effort cleanup of any stale container with the same name.
    subprocess.run(
        ["docker", "rm", "-f", container_name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )

    try:
        subprocess.run(
            [
                "docker", "run", "-d",
                "--name", container_name,
                "-p", f"{port}:8080",
                image,
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=300,  # image pull may take a while on a cold cache
        )
    except subprocess.CalledProcessError as e:
        pytest.skip(f"Failed to start Pathling container ({image}): {e.stderr.decode(errors='replace')}")
    except subprocess.TimeoutExpired:
        pytest.skip(f"Timed out pulling/starting Pathling image {image}")

    base_url = f"http://localhost:{port}/fhir"
    try:
        _wait_for_metadata(base_url, STARTUP_TIMEOUT_SECONDS)
        yield base_url
    finally:
        subprocess.run(
            ["docker", "rm", "-f", container_name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
