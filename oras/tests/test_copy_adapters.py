"""
Tests for the oras.copy.adapters module.

Tests RegistryTarget (with mocked Registry) and LayoutTarget (with real
filesystem fixtures from the ocilayout_data directory).
"""

import io
import os
import pathlib
import threading
from typing import BinaryIO, Dict, Optional
from unittest.mock import MagicMock, patch

import pytest

from oras.copy import (
    Descriptor,
    Mounter,
    ReadOnlyTarget,
    ReferencePusher,
    Target,
    copy,
)
from oras.copy.adapters import LayoutTarget, RegistryTarget
from oras.copy.descriptor import descriptors_equal

# Path to the OCI layout test fixtures
_OCILAYOUT1_DIR = os.path.join(
    os.path.dirname(__file__), "ocilayout_data", "ocilayout1"
)


# ---------------------------------------------------------------------------
# Test helper: In-memory Target for integration tests
# ---------------------------------------------------------------------------


class InMemoryTarget:
    """In-memory Target implementation for integration tests."""

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


# ---------------------------------------------------------------------------
# Test helpers: Mock Registry setup
# ---------------------------------------------------------------------------


def _make_mock_registry():
    """Create a mock Registry with a mock Container for testing."""
    registry = MagicMock()
    registry.prefix = "https"
    registry.headers = {}

    container = MagicMock()
    container.manifest_url.side_effect = (
        lambda tag: f"registry.example.com/v2/user/repo/manifests/{tag}"
    )
    container.upload_blob_url.return_value = (
        "registry.example.com/v2/user/repo/blobs/uploads/"
    )
    container.get_blob_url.side_effect = (
        lambda digest: f"registry.example.com/v2/user/repo/blobs/{digest}"
    )
    container.registry = "registry.example.com"

    registry.get_container.return_value = container
    registry._check_200_response = MagicMock()

    return registry, container


def _make_response(status_code=200, content=b"", headers=None):
    """Create a mock HTTP response."""
    response = MagicMock()
    response.status_code = status_code
    response.content = content
    response.headers = headers or {}
    return response


# ---------------------------------------------------------------------------
# Tests: RegistryTarget protocol conformance
# ---------------------------------------------------------------------------


class TestRegistryTargetProtocol:
    def test_is_target(self):
        registry, _ = _make_mock_registry()
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")
        assert isinstance(adapter, Target)

    def test_is_reference_pusher(self):
        registry, _ = _make_mock_registry()
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")
        assert isinstance(adapter, ReferencePusher)

    def test_is_mounter(self):
        registry, _ = _make_mock_registry()
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")
        assert isinstance(adapter, Mounter)

    def test_constructor_parses_container(self):
        registry, _ = _make_mock_registry()
        RegistryTarget(registry, "registry.example.com/user/repo:latest")
        registry.get_container.assert_called_once_with(
            "registry.example.com/user/repo:latest"
        )

    def test_constructor_loads_auth(self):
        registry, container = _make_mock_registry()
        RegistryTarget(registry, "registry.example.com/user/repo:latest")
        registry.auth.load_configs.assert_called_once_with(container)


# ---------------------------------------------------------------------------
# Tests: RegistryTarget.resolve
# ---------------------------------------------------------------------------


class TestRegistryTargetResolve:
    def test_resolve_returns_descriptor(self):
        registry, _ = _make_mock_registry()
        registry.do_request.return_value = _make_response(
            200,
            headers={
                "Content-Type": "application/vnd.oci.image.manifest.v1+json",
                "Docker-Content-Digest": "sha256:abc123",
                "Content-Length": "1234",
            },
        )
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")
        desc = adapter.resolve("v1.0")

        assert desc["mediaType"] == "application/vnd.oci.image.manifest.v1+json"
        assert desc["digest"] == "sha256:abc123"
        assert desc["size"] == 1234

    def test_resolve_uses_head_with_broad_accept(self):
        registry, _ = _make_mock_registry()
        registry.do_request.return_value = _make_response(
            200,
            headers={
                "Content-Type": "application/vnd.oci.image.manifest.v1+json",
                "Docker-Content-Digest": "sha256:abc",
                "Content-Length": "100",
            },
        )
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")
        adapter.resolve("latest")

        call_args = registry.do_request.call_args
        assert call_args[0][1] == "HEAD"  # method
        headers = call_args[1].get("headers") or call_args[0][2] if len(call_args[0]) > 2 else call_args[1].get("headers")
        assert "application/vnd.oci.image.manifest.v1+json" in headers["Accept"]
        assert "application/vnd.oci.image.index.v1+json" in headers["Accept"]


# ---------------------------------------------------------------------------
# Tests: RegistryTarget.fetch
# ---------------------------------------------------------------------------


class TestRegistryTargetFetch:
    def test_fetch_blob(self):
        registry, container = _make_mock_registry()
        blob_content = b"blob data here"
        registry.get_blob.return_value = _make_response(200, content=blob_content)
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")

        desc = {
            "mediaType": "application/octet-stream",
            "digest": "sha256:blobdigest",
            "size": len(blob_content),
        }
        result = adapter.fetch(desc)
        assert result.read() == blob_content
        registry.get_blob.assert_called_once_with(container, "sha256:blobdigest")

    def test_fetch_manifest(self):
        registry, _ = _make_mock_registry()
        manifest_bytes = b'{"schemaVersion":2}'
        registry.do_request.return_value = _make_response(
            200, content=manifest_bytes
        )
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")

        desc = {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": "sha256:manifestdigest",
            "size": len(manifest_bytes),
        }
        result = adapter.fetch(desc)
        assert result.read() == manifest_bytes

        # Verify it used do_request with GET and Accept header
        call_args = registry.do_request.call_args
        assert "GET" in call_args[0]
        headers = call_args[1].get("headers", {})
        assert headers["Accept"] == "application/vnd.oci.image.manifest.v1+json"


# ---------------------------------------------------------------------------
# Tests: RegistryTarget.exists
# ---------------------------------------------------------------------------


class TestRegistryTargetExists:
    def test_exists_blob_true(self):
        registry, container = _make_mock_registry()
        registry.blob_exists.return_value = True
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")

        desc = {
            "mediaType": "application/octet-stream",
            "digest": "sha256:exists",
            "size": 100,
        }
        assert adapter.exists(desc) is True
        registry.blob_exists.assert_called_once_with(desc, container)

    def test_exists_blob_false(self):
        registry, _ = _make_mock_registry()
        registry.blob_exists.return_value = False
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")

        desc = {
            "mediaType": "application/octet-stream",
            "digest": "sha256:missing",
            "size": 100,
        }
        assert adapter.exists(desc) is False

    def test_exists_manifest_true(self):
        registry, _ = _make_mock_registry()
        registry.do_request.return_value = _make_response(200)
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")

        desc = {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": "sha256:mfst",
            "size": 500,
        }
        assert adapter.exists(desc) is True

    def test_exists_manifest_false(self):
        registry, _ = _make_mock_registry()
        registry.do_request.return_value = _make_response(404)
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")

        desc = {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": "sha256:missing",
            "size": 500,
        }
        assert adapter.exists(desc) is False


# ---------------------------------------------------------------------------
# Tests: RegistryTarget.push
# ---------------------------------------------------------------------------


class TestRegistryTargetPush:
    def test_push_blob_uses_upload_blob(self):
        registry, container = _make_mock_registry()
        registry.upload_blob.return_value = _make_response(201)
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")

        desc = {
            "mediaType": "application/octet-stream",
            "digest": "sha256:pushblob",
            "size": 5,
        }
        adapter.push(desc, io.BytesIO(b"hello"))

        # upload_blob should have been called with a temp file path
        assert registry.upload_blob.call_count == 1
        call_args = registry.upload_blob.call_args
        tmp_path = call_args[0][0]
        assert isinstance(tmp_path, str)
        # Temp file should be cleaned up
        assert not os.path.exists(tmp_path)

    def test_push_manifest_uses_put(self):
        registry, _ = _make_mock_registry()
        registry.do_request.return_value = _make_response(201)
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")

        desc = {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": "sha256:pushmfst",
            "size": 19,
        }
        adapter.push(desc, io.BytesIO(b'{"schemaVersion":2}'))

        call_args = registry.do_request.call_args
        assert "PUT" in call_args[0]
        headers = call_args[1].get("headers", {})
        assert headers["Content-Type"] == "application/vnd.oci.image.manifest.v1+json"
        assert call_args[1].get("data") == b'{"schemaVersion":2}'


# ---------------------------------------------------------------------------
# Tests: RegistryTarget.tag
# ---------------------------------------------------------------------------


class TestRegistryTargetTag:
    def test_tag_fetches_then_puts(self):
        registry, _ = _make_mock_registry()
        manifest_bytes = b'{"schemaVersion":2}'

        # First call is fetch (GET), second is tag (PUT)
        registry.do_request.side_effect = [
            _make_response(200, content=manifest_bytes),  # fetch
            _make_response(201),  # tag PUT
        ]
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")

        desc = {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": "sha256:tagtest",
            "size": len(manifest_bytes),
        }
        adapter.tag(desc, "v2.0")

        assert registry.do_request.call_count == 2
        # Second call should be PUT to the tag URL
        put_call = registry.do_request.call_args_list[1]
        assert "PUT" in put_call[0]
        assert "v2.0" in put_call[0][0]  # URL contains the tag


# ---------------------------------------------------------------------------
# Tests: RegistryTarget.push_reference
# ---------------------------------------------------------------------------


class TestRegistryTargetPushReference:
    def test_push_reference_puts_to_tag_url(self):
        registry, _ = _make_mock_registry()
        registry.do_request.return_value = _make_response(201)
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")

        desc = {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": "sha256:refpush",
            "size": 19,
        }
        adapter.push_reference(desc, io.BytesIO(b'{"schemaVersion":2}'), "v3.0")

        call_args = registry.do_request.call_args
        assert "PUT" in call_args[0]
        assert "v3.0" in call_args[0][0]
        headers = call_args[1].get("headers", {})
        assert headers["Content-Type"] == "application/vnd.oci.image.manifest.v1+json"


# ---------------------------------------------------------------------------
# Tests: RegistryTarget.mount
# ---------------------------------------------------------------------------


class TestRegistryTargetMount:
    def test_mount_success_201(self):
        registry, _ = _make_mock_registry()
        registry.do_request.return_value = _make_response(201)
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")

        desc = {
            "mediaType": "application/octet-stream",
            "digest": "sha256:mountblob",
            "size": 100,
        }
        get_content = MagicMock()
        adapter.mount(desc, "other/repo", get_content)

        # get_content should NOT have been called (mount succeeded)
        get_content.assert_not_called()

        # Verify POST had mount and from params in the URL
        call_args = registry.do_request.call_args
        url = call_args[0][0]
        assert "mount=sha256%3Amountblob" in url or "mount=sha256:mountblob" in url
        assert "from=other" in url

    def test_mount_fallback_202(self):
        registry, container = _make_mock_registry()
        # First call: POST returns 202 (mount failed)
        # Second call: PUT to complete the upload
        registry.do_request.side_effect = [
            _make_response(202, headers={"location": "/v2/user/repo/blobs/uploads/session123"}),
            _make_response(201),
        ]
        registry._get_location.return_value = (
            "https://registry.example.com/v2/user/repo/blobs/uploads/session123"
        )
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")

        desc = {
            "mediaType": "application/octet-stream",
            "digest": "sha256:fallbackblob",
            "size": 5,
        }
        get_content = MagicMock(return_value=io.BytesIO(b"hello"))
        adapter.mount(desc, "other/repo", get_content)

        # get_content should have been called (mount failed, fallback)
        get_content.assert_called_once()

        # Second do_request call should be a PUT
        put_call = registry.do_request.call_args_list[1]
        assert "PUT" in put_call[0]

    def test_mount_fallback_no_session_url(self):
        """Mount fallback raises ValueError when no session URL is returned."""
        registry, _ = _make_mock_registry()
        registry.do_request.return_value = _make_response(202)
        registry._get_location.return_value = ""
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")

        desc = {
            "mediaType": "application/octet-stream",
            "digest": "sha256:nosession",
            "size": 5,
        }
        get_content = MagicMock(return_value=io.BytesIO(b"hello"))
        with pytest.raises(ValueError, match="no session URL"):
            adapter.mount(desc, "other/repo", get_content)


# ---------------------------------------------------------------------------
# Tests: RegistryTarget.push blob temp file cleanup on OSError
# ---------------------------------------------------------------------------


class TestRegistryTargetPushCleanup:
    def test_push_blob_cleanup_oserror_is_silent(self):
        """OSError during temp file cleanup should not propagate."""
        registry, _ = _make_mock_registry()
        registry.upload_blob.return_value = _make_response(201)
        adapter = RegistryTarget(registry, "registry.example.com/user/repo:latest")

        desc = {
            "mediaType": "application/octet-stream",
            "digest": "sha256:cleanuptest",
            "size": 5,
        }
        # Patch os.unlink to raise OSError
        with patch("oras.copy.adapters.os.unlink", side_effect=OSError("fake")):
            # Should not raise
            adapter.push(desc, io.BytesIO(b"hello"))

        assert registry.upload_blob.call_count == 1


# ---------------------------------------------------------------------------
# Tests: LayoutTarget protocol conformance
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not os.path.isdir(_OCILAYOUT1_DIR),
    reason="ocilayout1 test fixture not found",
)
class TestLayoutTargetProtocol:
    def test_is_read_only_target(self):
        from oras.layout.layout import Layout

        layout = Layout(_OCILAYOUT1_DIR)
        adapter = LayoutTarget(layout)
        assert isinstance(adapter, ReadOnlyTarget)


# ---------------------------------------------------------------------------
# Tests: LayoutTarget.resolve
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not os.path.isdir(_OCILAYOUT1_DIR),
    reason="ocilayout1 test fixture not found",
)
class TestLayoutTargetResolve:
    def test_resolve_latest(self):
        from oras.layout.layout import Layout

        layout = Layout(_OCILAYOUT1_DIR)
        adapter = LayoutTarget(layout)
        desc = adapter.resolve("latest")

        assert desc["mediaType"] == "application/vnd.oci.image.manifest.v1+json"
        assert (
            desc["digest"]
            == "sha256:cfcb44ade8c9b2579247ceec82c2f18bf03d956b9b2c050753b7d47d1edd369d"
        )
        assert desc["size"] == 567

    def test_resolve_missing_reference(self):
        from oras.layout.layout import Layout

        layout = Layout(_OCILAYOUT1_DIR)
        adapter = LayoutTarget(layout)
        with pytest.raises(FileNotFoundError, match="nonexistent"):
            adapter.resolve("nonexistent")


# ---------------------------------------------------------------------------
# Tests: LayoutTarget.fetch
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not os.path.isdir(_OCILAYOUT1_DIR),
    reason="ocilayout1 test fixture not found",
)
class TestLayoutTargetFetch:
    def test_fetch_manifest(self):
        from oras.layout.layout import Layout

        layout = Layout(_OCILAYOUT1_DIR)
        adapter = LayoutTarget(layout)

        desc = {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": "sha256:cfcb44ade8c9b2579247ceec82c2f18bf03d956b9b2c050753b7d47d1edd369d",
            "size": 567,
        }
        rc = adapter.fetch(desc)
        data = rc.read()
        rc.close()
        assert len(data) == 567
        assert b"schemaVersion" in data

    def test_fetch_blob_matches_disk(self):
        from oras.layout.layout import Layout

        layout = Layout(_OCILAYOUT1_DIR)
        adapter = LayoutTarget(layout)

        # Fetch the config blob
        desc = {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:6a315ec0732bc64a9763b6da6df8326f836c3661991c8ba3f5e83e1ad4fd57b7",
            "size": 540,
        }
        rc = adapter.fetch(desc)
        data = rc.read()
        rc.close()

        # Compare with file on disk
        blob_path = os.path.join(
            _OCILAYOUT1_DIR,
            "blobs",
            "sha256",
            "6a315ec0732bc64a9763b6da6df8326f836c3661991c8ba3f5e83e1ad4fd57b7",
        )
        with open(blob_path, "rb") as f:
            expected = f.read()
        assert data == expected


# ---------------------------------------------------------------------------
# Tests: LayoutTarget.exists
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not os.path.isdir(_OCILAYOUT1_DIR),
    reason="ocilayout1 test fixture not found",
)
class TestLayoutTargetExists:
    def test_exists_present_blob(self):
        from oras.layout.layout import Layout

        layout = Layout(_OCILAYOUT1_DIR)
        adapter = LayoutTarget(layout)

        desc = {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:6a315ec0732bc64a9763b6da6df8326f836c3661991c8ba3f5e83e1ad4fd57b7",
            "size": 540,
        }
        assert adapter.exists(desc) is True

    def test_exists_missing_blob(self):
        from oras.layout.layout import Layout

        layout = Layout(_OCILAYOUT1_DIR)
        adapter = LayoutTarget(layout)

        desc = {
            "mediaType": "application/octet-stream",
            "digest": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            "size": 0,
        }
        assert adapter.exists(desc) is False


# ---------------------------------------------------------------------------
# Tests: Integration — copy from LayoutTarget to InMemoryTarget
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not os.path.isdir(_OCILAYOUT1_DIR),
    reason="ocilayout1 test fixture not found",
)
class TestLayoutToMemoryIntegration:
    def test_copy_layout_to_memory(self):
        """Full copy of ocilayout1's 'latest' tag into an InMemoryTarget."""
        from oras.layout.layout import Layout

        layout = Layout(_OCILAYOUT1_DIR)
        src = LayoutTarget(layout)
        dst = InMemoryTarget()

        root = copy(src, "latest", dst, "latest")

        # Root descriptor should match the manifest
        assert root["mediaType"] == "application/vnd.oci.image.manifest.v1+json"
        assert (
            root["digest"]
            == "sha256:cfcb44ade8c9b2579247ceec82c2f18bf03d956b9b2c050753b7d47d1edd369d"
        )

        # Manifest should be at destination
        assert dst.exists(root)

        # Config blob should be at destination
        config_desc = {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:6a315ec0732bc64a9763b6da6df8326f836c3661991c8ba3f5e83e1ad4fd57b7",
            "size": 540,
        }
        assert dst.exists(config_desc)

        # Layer blob should be at destination
        layer_desc = {
            "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
            "digest": "sha256:1a88c78449cd2ce9961de409273deac250a60e55b1d8c4beef858b8618ddaba5",
            "size": 125,
        }
        assert dst.exists(layer_desc)

        # Tag should be set
        tagged = dst.get_tag("latest")
        assert tagged is not None
        assert descriptors_equal(tagged, root)

    def test_copy_layout_content_matches_source(self):
        """Verify copied content byte-for-byte matches layout files."""
        from oras.layout.layout import Layout

        layout = Layout(_OCILAYOUT1_DIR)
        src = LayoutTarget(layout)
        dst = InMemoryTarget()

        copy(src, "latest", dst, "latest")

        # Check config content matches
        config_digest = "sha256:6a315ec0732bc64a9763b6da6df8326f836c3661991c8ba3f5e83e1ad4fd57b7"
        config_path = os.path.join(
            _OCILAYOUT1_DIR, "blobs", "sha256",
            "6a315ec0732bc64a9763b6da6df8326f836c3661991c8ba3f5e83e1ad4fd57b7",
        )
        with open(config_path, "rb") as f:
            expected = f.read()
        assert dst.get_content(config_digest) == expected

        # Check layer content matches
        layer_digest = "sha256:1a88c78449cd2ce9961de409273deac250a60e55b1d8c4beef858b8618ddaba5"
        layer_path = os.path.join(
            _OCILAYOUT1_DIR, "blobs", "sha256",
            "1a88c78449cd2ce9961de409273deac250a60e55b1d8c4beef858b8618ddaba5",
        )
        with open(layer_path, "rb") as f:
            expected = f.read()
        assert dst.get_content(layer_digest) == expected
