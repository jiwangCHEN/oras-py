"""
In-memory and caching content storage implementations.

Concrete realizations of the storage protocols in
:mod:`oras.content.storage`. ``MemoryStorage`` is a standalone in-memory
store; ``CacheProxy`` is the read-through metadata cache the copy engine
wraps around a source; ``FetcherFunc`` adapts a plain callable into a
fetcher.

Matches oras-go's content.Memory and internal/cas.Proxy.
"""

__author__ = "The ORAS Authors"
__copyright__ = "Copyright The ORAS Authors."
__license__ = "Apache-2.0"

import io
import threading
from typing import BinaryIO, Callable

from oras.content.storage import ReadOnlyStorage, Storage
from oras.types import Descriptor


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
