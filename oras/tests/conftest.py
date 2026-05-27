import io
import os
import threading
from dataclasses import dataclass
from typing import BinaryIO, Dict, Optional

import pytest

from oras.types import Descriptor


class InMemoryTarget:
    """
    Thread-safe in-memory Target for copy engine tests.

    Implements the full Target protocol (fetch, exists, push, tag, resolve)
    using plain dicts backed by a threading.Lock. Raises FileExistsError
    on duplicate push so copy engine idempotency handling is exercised.
    """

    def __init__(self):
        self._content: Dict[str, bytes] = {}
        self._tags: Dict[str, Descriptor] = {}
        self._lock = threading.Lock()

    def fetch(self, desc: Descriptor) -> BinaryIO:
        digest = desc.get("digest", "")
        with self._lock:
            data = self._content.get(digest)
        if data is None:
            raise FileNotFoundError(f"content not found: {digest}")
        return io.BytesIO(data)

    def exists(self, desc: Descriptor) -> bool:
        digest = desc.get("digest", "")
        with self._lock:
            return digest in self._content

    def push(self, desc: Descriptor, content: BinaryIO) -> None:
        data = content.read()
        digest = desc.get("digest", "")
        with self._lock:
            if digest in self._content:
                raise FileExistsError(f"content already exists: {digest}")
            self._content[digest] = data

    def tag(self, desc: Descriptor, reference: str) -> None:
        with self._lock:
            self._tags[reference] = desc

    def resolve(self, reference: str) -> Descriptor:
        with self._lock:
            desc = self._tags.get(reference)
        if desc is None:
            raise FileNotFoundError(f"reference not found: {reference}")
        return desc

    def get_content(self, digest: str) -> bytes:
        with self._lock:
            return self._content.get(digest, b"")

    def get_tag(self, reference: str) -> Optional[Descriptor]:
        with self._lock:
            return self._tags.get(reference)


@dataclass
class TestCredentials:
    with_auth: bool
    user: str
    password: str


@pytest.fixture
def registry():
    host = os.environ.get("ORAS_HOST")
    port = os.environ.get("ORAS_PORT")

    if not host or not port:
        pytest.skip(
            "You must export ORAS_HOST and ORAS_PORT"
            " for a running registry before running tests."
        )

    return f"{host}:{port}"


@pytest.fixture
def credentials(request):
    with_auth = os.environ.get("ORAS_AUTH") == "true"
    user = os.environ.get("ORAS_USER", "myuser")
    pwd = os.environ.get("ORAS_PASS", "mypass")

    if with_auth and not user or not pwd:
        pytest.skip("To test auth you need to export ORAS_USER and ORAS_PASS")

    marks = [m.name for m in request.node.iter_markers()]
    if request.node.parent:
        marks += [m.name for m in request.node.parent.iter_markers()]

    if request.node.get_closest_marker("with_auth"):
        if request.node.get_closest_marker("with_auth").args[0] != with_auth:
            if with_auth:
                pytest.skip("test requires un-authenticated access to registry")
            else:
                pytest.skip("test requires authenticated access to registry")

    return TestCredentials(with_auth, user, pwd)


@pytest.fixture
def target(registry):
    return f"{registry}/dinosaur/artifact:v1"


@pytest.fixture
def target_dir(registry):
    return f"{registry}/dinosaur/directory:v1"


@pytest.fixture
def target_layout_single(registry):
    """Target for single-arch OCI layout push tests"""
    return f"{registry}/dinosaur/layout-single:v1"


@pytest.fixture
def target_layout_multi(registry):
    """Target for multi-arch OCI layout push tests"""
    return f"{registry}/dinosaur/layout-multi:v1"
