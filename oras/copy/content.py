"""
Content interfaces and implementations for the copy engine.

Defines Protocol classes for content storage operations (matching oras-go's
content package), plus concrete implementations for in-memory storage and
caching proxy.
"""

__author__ = "The ORAS Authors"
__copyright__ = "Copyright The ORAS Authors."
__license__ = "Apache-2.0"

import io
import json
import threading
from typing import BinaryIO, Callable, List, Optional, Protocol, Tuple, runtime_checkable

from oras.copy.descriptor import Descriptor


# ---------------------------------------------------------------------------
# Content Protocols (matching oras-go content package)
# ---------------------------------------------------------------------------


@runtime_checkable
class Fetcher(Protocol):
    """Fetches content for a given descriptor."""

    def fetch(self, desc: Descriptor) -> BinaryIO: ...


@runtime_checkable
class Pusher(Protocol):
    """Pushes content for a given descriptor."""

    def push(self, desc: Descriptor, content: BinaryIO) -> None: ...


@runtime_checkable
class ReadOnlyStorage(Protocol):
    """Read-only content-addressable storage."""

    def fetch(self, desc: Descriptor) -> BinaryIO: ...
    def exists(self, desc: Descriptor) -> bool: ...


@runtime_checkable
class Storage(Protocol):
    """Content-addressable storage with read and write."""

    def fetch(self, desc: Descriptor) -> BinaryIO: ...
    def exists(self, desc: Descriptor) -> bool: ...
    def push(self, desc: Descriptor, content: BinaryIO) -> None: ...


@runtime_checkable
class Resolver(Protocol):
    """Resolves a reference string to a descriptor."""

    def resolve(self, reference: str) -> Descriptor: ...


@runtime_checkable
class Tagger(Protocol):
    """Tags a descriptor with a reference string."""

    def tag(self, desc: Descriptor, reference: str) -> None: ...


@runtime_checkable
class TagResolver(Protocol):
    """Combined tagger and resolver."""

    def tag(self, desc: Descriptor, reference: str) -> None: ...
    def resolve(self, reference: str) -> Descriptor: ...


# ---------------------------------------------------------------------------
# Target Protocols (matching oras-go target.go)
# ---------------------------------------------------------------------------


@runtime_checkable
class Target(Protocol):
    """A target that supports full read/write storage + tag/resolve."""

    def fetch(self, desc: Descriptor) -> BinaryIO: ...
    def exists(self, desc: Descriptor) -> bool: ...
    def push(self, desc: Descriptor, content: BinaryIO) -> None: ...
    def tag(self, desc: Descriptor, reference: str) -> None: ...
    def resolve(self, reference: str) -> Descriptor: ...


@runtime_checkable
class ReadOnlyTarget(Protocol):
    """A target that supports read-only storage + resolve."""

    def fetch(self, desc: Descriptor) -> BinaryIO: ...
    def exists(self, desc: Descriptor) -> bool: ...
    def resolve(self, reference: str) -> Descriptor: ...


# ---------------------------------------------------------------------------
# Registry-Specific Protocols (matching oras-go registry package)
# ---------------------------------------------------------------------------


@runtime_checkable
class ReferencePusher(Protocol):
    """Pushes content with a reference tag atomically."""

    def push_reference(
        self, desc: Descriptor, content: BinaryIO, reference: str
    ) -> None: ...


@runtime_checkable
class ReferenceFetcher(Protocol):
    """Fetches content by reference, returning descriptor and content."""

    def fetch_reference(self, reference: str) -> Tuple[Descriptor, BinaryIO]: ...


@runtime_checkable
class Mounter(Protocol):
    """Mounts a blob from another repository, with fallback to copy."""

    def mount(
        self,
        desc: Descriptor,
        from_repo: str,
        get_content: Callable[[], BinaryIO],
    ) -> None: ...


# ---------------------------------------------------------------------------
# FetcherFunc: Adapter from callable to Fetcher
# ---------------------------------------------------------------------------


class FetcherFunc:
    """Wraps a callable as a Fetcher."""

    def __init__(self, fn: Callable[[Descriptor], BinaryIO]):
        self._fn = fn

    def fetch(self, desc: Descriptor) -> BinaryIO:
        return self._fn(desc)


# ---------------------------------------------------------------------------
# MemoryStorage: Thread-safe in-memory content-addressable storage
# ---------------------------------------------------------------------------


class MemoryStorage:
    """Thread-safe in-memory content storage keyed by digest."""

    def __init__(self):
        self._lock = threading.Lock()
        self._content: dict = {}  # digest -> bytes

    def exists(self, desc: Descriptor) -> bool:
        digest = desc.get("digest", "")
        with self._lock:
            return digest in self._content

    def fetch(self, desc: Descriptor) -> BinaryIO:
        digest = desc.get("digest", "")
        with self._lock:
            data = self._content.get(digest)
        if data is None:
            raise FileNotFoundError(f"content not found: {digest}")
        return io.BytesIO(data)

    def push(self, desc: Descriptor, content: BinaryIO) -> None:
        data = content.read()
        digest = desc.get("digest", "")
        with self._lock:
            self._content[digest] = data


# ---------------------------------------------------------------------------
# CacheProxy: Caching read-through proxy for content storage
# ---------------------------------------------------------------------------


class CacheProxy:
    """
    Caching proxy that wraps a ReadOnlyStorage with a local Storage cache.

    Non-leaf nodes (manifests, indexes) are cached in memory for reuse
    during graph traversal. The cache has a byte size limit to prevent
    unbounded memory growth.

    Matches oras-go's internal/cas.Proxy.
    """

    def __init__(self, base: ReadOnlyStorage, cache: Storage, max_bytes: int):
        self.base = base
        self.cache = cache
        self.max_bytes = max_bytes
        self.stop_caching: bool = False
        self._cached_bytes: int = 0
        self._lock = threading.Lock()

    def exists(self, desc: Descriptor) -> bool:
        if self.cache.exists(desc):
            return True
        return self.base.exists(desc)

    def fetch(self, desc: Descriptor) -> BinaryIO:
        # Try cache first (single call avoids TOCTOU)
        try:
            return self.cache.fetch(desc)
        except FileNotFoundError:
            pass

        # Fetch from base
        stream = self.base.fetch(desc)

        if self.stop_caching:
            return stream

        size = desc.get("size", 0)
        with self._lock:
            if self._cached_bytes + size > self.max_bytes:
                return stream
            self._cached_bytes += size

        # Read, cache, and return
        data = stream.read()
        self.cache.push(desc, io.BytesIO(data))
        return io.BytesIO(data)

    def push(self, desc: Descriptor, content: BinaryIO) -> None:
        """Push delegates to the base (not the cache)."""
        raise NotImplementedError("CacheProxy is read-only; push to base directly")


# ---------------------------------------------------------------------------
# Content utility functions
# ---------------------------------------------------------------------------


def successors(fetcher, desc: Descriptor) -> List[Descriptor]:
    """
    Find the successors (child nodes) of an OCI descriptor.

    For manifests: returns config + layers + subject.
    For indexes: returns manifests + subject.
    For blobs (leaf nodes): returns empty list.

    Matches oras-go's content.Successors.
    """
    media_type = desc.get("mediaType", "")

    # OCI Image Manifest
    if media_type == "application/vnd.oci.image.manifest.v1+json":
        return _manifest_successors(fetcher, desc)

    # OCI Image Index
    if media_type == "application/vnd.oci.image.index.v1+json":
        return _index_successors(fetcher, desc)

    # Docker Manifest v2
    if media_type == "application/vnd.docker.distribution.manifest.v2+json":
        return _manifest_successors(fetcher, desc)

    # Docker Manifest List
    if media_type == "application/vnd.docker.distribution.manifest.list.v2+json":
        return _index_successors(fetcher, desc)

    # Leaf node (blobs, configs, etc.)
    return []


def _manifest_successors(fetcher, desc: Descriptor) -> List[Descriptor]:
    """Extract successors from a manifest (config + layers + subject)."""
    data = fetcher.fetch(desc)
    manifest = json.loads(data.read())
    result: List[Descriptor] = []
    if "config" in manifest and manifest["config"]:
        result.append(manifest["config"])
    result.extend(manifest.get("layers", []))
    if "subject" in manifest and manifest["subject"]:
        result.append(manifest["subject"])
    return result


def _index_successors(fetcher, desc: Descriptor) -> List[Descriptor]:
    """Extract successors from an index (manifests + subject)."""
    data = fetcher.fetch(desc)
    index = json.loads(data.read())
    result: List[Descriptor] = list(index.get("manifests", []))
    if "subject" in index and index["subject"]:
        result.append(index["subject"])
    return result
