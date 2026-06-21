"""
Storage and target interfaces for the copy engine.

Defines the Protocol classes (the "from/to" storage + target contract) that
the copy engine checks against. Concrete realizations live in
:mod:`oras.content.memory` (in-memory store + read-through cache) and in the
target adapters :mod:`oras.content.registry` and :mod:`oras.content.layout`; the
copy algorithm in :mod:`oras.copy` consumes these protocols.

These mirror the relevant subset of oras-go's content package interfaces.
Only the protocols the engine actually uses are defined here:

* Type hints: ``ReadOnlyStorage``, ``Storage``, ``ReadOnlyTarget``, ``Target``
* Runtime capability dispatch (``isinstance``): ``ReferenceFetcher``,
  ``ReferencePusher``, ``Mounter``

Note: ``@runtime_checkable`` only verifies the *presence* of methods, not
their signatures (PEP 544). These protocols document intent and drive
capability detection; they do not enforce shape.
"""

__author__ = "The ORAS Authors"
__copyright__ = "Copyright The ORAS Authors."
__license__ = "Apache-2.0"

from typing import BinaryIO, Callable, Protocol, Tuple, runtime_checkable

from oras.types import Descriptor


# ---------------------------------------------------------------------------
# Storage Protocols (content-addressable, keyed by digest)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Target Protocols (storage + reference resolution / tagging)
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
# Optional capability Protocols (detected at runtime via isinstance)
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
