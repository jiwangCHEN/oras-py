"""
Core graph copy algorithm for the copy engine.

Implements the concurrent DAG traversal that copies content from
a source to a destination, with deduplication, existence checking,
optional blob mounting, and foreign layer filtering.

Matches oras-go's copyGraph, copyNode, mountOrCopyNode, and doCopyNode.
"""

__author__ = "The ORAS Authors"
__copyright__ = "Copyright The ORAS Authors."
__license__ = "Apache-2.0"

import hashlib
import io
import threading
from typing import Callable, List, Optional

from oras.copy import content as content_mod
from oras.copy.descriptor import (
    Descriptor,
    descriptors_equal,
    is_manifest,
    remove_foreign_layers,
)
from oras.copy.errors import CopyError, CopyErrorOrigin
from oras.copy.options import (
    DEFAULT_CONCURRENCY,
    DEFAULT_MAX_METADATA_BYTES,
    CopyGraphOptions,
)
from oras.copy.tracker import StatusTracker

# Sentinel exception class: raise SkipNode() from pre_copy to signal
# that the descriptor was already handled and should be skipped.
class SkipNode(Exception):
    pass


class LimitedRegion:
    """
    A concurrency slot that can be temporarily released and re-acquired.

    Matches oras-go's syncutil.LimitedRegion. When a non-leaf node
    discovers children, it calls end() to release its semaphore slot
    so children can use it, then start() to re-acquire after children
    complete. This prevents deadlock where all slots are held by
    parents waiting for children that can never acquire a slot.
    """

    def __init__(self, semaphore: threading.Semaphore):
        self._semaphore = semaphore
        self._ended = False

    def end(self) -> None:
        """Release the semaphore slot if currently held."""
        if not self._ended:
            self._semaphore.release()
            self._ended = True

    def start(self) -> None:
        """Re-acquire the semaphore slot if previously released."""
        if self._ended:
            self._semaphore.acquire()
            self._ended = False


def _go(
    semaphore: threading.Semaphore,
    fn: Callable[[LimitedRegion, Descriptor], None],
    items: List[Descriptor],
    cancel_event: threading.Event,
) -> None:
    """
    Execute fn for each item with semaphore-gated concurrency.

    Matches oras-go's syncutil.Go. For each item:
    1. Acquire the semaphore (blocks until a slot is available)
    2. Spawn a thread that runs fn(region, item)
    3. The thread releases the semaphore slot when done (via region.end())

    Threads are lightweight here (not a bounded pool), matching Go's
    goroutine model. The semaphore is the concurrency bottleneck, not
    the number of threads.
    """
    if not items:
        return

    errors: list = []
    error_lock = threading.Lock()
    threads: list = []

    for item in items:
        if cancel_event.is_set():
            break
        semaphore.acquire()
        region = LimitedRegion(semaphore)

        def worker(r=region, it=item):
            try:
                if not cancel_event.is_set():
                    fn(r, it)
            except Exception as e:
                with error_lock:
                    errors.append(e)
                cancel_event.set()
            finally:
                r.end()

        t = threading.Thread(target=worker, daemon=True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    if errors:
        raise errors[0]


def copy_graph(
    src: content_mod.ReadOnlyStorage,
    dst: content_mod.Storage,
    root: Descriptor,
    proxy: Optional[content_mod.CacheProxy] = None,
    opts: Optional[CopyGraphOptions] = None,
) -> None:
    """
    Copy a directed acyclic graph (DAG) of OCI content from src to dst.

    Starting from the root descriptor, traverses the content graph
    concurrently, copying each node exactly once. Non-leaf nodes
    (manifests, indexes) are copied after all their children.

    Uses a semaphore + LimitedRegion pattern (matching oras-go) to
    prevent deadlocks: parents release their concurrency slot while
    waiting for children, then re-acquire before copying.

    Matches oras-go's copyGraph function.

    Args:
        src: Source content storage (read-only).
        dst: Destination content storage (read-write).
        root: Root descriptor to start the copy from.
        proxy: Optional caching proxy. Created automatically if None.
        opts: Copy graph options. Uses defaults if None.
    """
    if opts is None:
        opts = CopyGraphOptions()

    # Initialize proxy
    if proxy is None:
        max_bytes = opts.max_metadata_bytes
        if max_bytes <= 0:
            max_bytes = DEFAULT_MAX_METADATA_BYTES
        proxy = content_mod.CacheProxy(
            src, content_mod.MemoryStorage(), max_bytes
        )

    # Initialize concurrency via semaphore (not a bounded thread pool,
    # which would deadlock on recursive submissions)
    concurrency = opts.concurrency if opts.concurrency > 0 else DEFAULT_CONCURRENCY
    semaphore = threading.Semaphore(concurrency)

    # Initialize tracker for deduplication
    tracker = StatusTracker()

    # Choose successor discovery function
    find_successors = opts.find_successors or content_mod.successors

    # Shared error state for cancellation
    cancel_event = threading.Event()
    first_error = None
    error_lock = threading.Lock()

    def _set_error(err: Exception) -> None:
        nonlocal first_error
        with error_lock:
            if first_error is None:
                first_error = err
        cancel_event.set()

    def _process_node(region: LimitedRegion, desc: Descriptor) -> None:
        """Process a single node in the content DAG."""
        if cancel_event.is_set():
            return

        # Deduplication: try to claim this descriptor
        done, committed = tracker.try_commit(desc)
        if not committed:
            # Another worker is handling this; wait for it
            done.wait()
            return

        try:
            # Check if content already exists in destination
            try:
                exists = dst.exists(desc)
            except Exception as e:
                raise CopyError("Exists", CopyErrorOrigin.DESTINATION, e)

            if exists:
                if opts.on_copy_skipped is not None:
                    opts.on_copy_skipped(desc)
                return

            # Find child nodes (successors) via the caching proxy
            try:
                children = find_successors(proxy, desc)
            except Exception as e:
                raise CopyError("FindSuccessors", CopyErrorOrigin.SOURCE, e)

            children = remove_foreign_layers(children)

            if children:
                # Release semaphore slot so children can use it
                region.end()

                # Spawn children concurrently, each acquiring their own slot
                _go(semaphore, _process_node, children, cancel_event)

                if cancel_event.is_set():
                    return

                # Verify all children completed successfully
                for child in children:
                    child_done, child_committed = tracker.try_commit(child)
                    if child_committed:
                        raise RuntimeError(
                            f"{desc.get('digest')}: "
                            f"{child.get('digest')}: successor not committed"
                        )
                    child_done.wait()

                # Re-acquire semaphore slot before copying this node
                region.start()

            # All children are done; copy this node
            if cancel_event.is_set():
                return
            if proxy.cache.exists(desc):
                _copy_node(proxy.cache, dst, desc, opts)
            else:
                _mount_or_copy_node(src, dst, desc, opts)
        except Exception as e:
            _set_error(e)
        finally:
            done.set()

    # Start the root via _go (acquires semaphore, spawns thread, releases on done)
    _go(semaphore, _process_node, [root], cancel_event)

    # Propagate any error from the concurrent workers
    if first_error is not None:
        raise first_error


def _copy_node(
    src: content_mod.ReadOnlyStorage,
    dst: content_mod.Storage,
    desc: Descriptor,
    opts: CopyGraphOptions,
) -> None:
    """
    Copy a single node from src to dst, invoking pre/post copy hooks.

    Matches oras-go's copyNode.
    """
    if opts.pre_copy is not None:
        try:
            opts.pre_copy(desc)
        except Exception as e:
            if isinstance(e, SkipNode):
                return
            raise

    _do_copy_node(src, dst, desc)

    if opts.post_copy is not None:
        opts.post_copy(desc)


def _verify_digest(data: bytes, expected_digest: str) -> None:
    """Verify fetched content matches the expected digest."""
    if not expected_digest or ":" not in expected_digest:
        return
    algorithm, expected_hash = expected_digest.split(":", 1)
    try:
        h = hashlib.new(algorithm)
    except ValueError:
        return
    h.update(data)
    actual_hash = h.hexdigest()
    if actual_hash != expected_hash:
        raise CopyError(
            "VerifyDigest",
            CopyErrorOrigin.SOURCE,
            ValueError(
                f"digest mismatch: expected {expected_digest}, "
                f"got {algorithm}:{actual_hash}"
            ),
        )


def _do_copy_node(
    src: content_mod.ReadOnlyStorage,
    dst: content_mod.Storage,
    desc: Descriptor,
) -> None:
    """
    Fetch content from src and push to dst.

    Verifies fetched content matches the descriptor's digest.
    ErrAlreadyExists on push is silently ignored (idempotent).
    Matches oras-go's doCopyNode.
    """
    try:
        rc = src.fetch(desc)
    except Exception as e:
        raise CopyError("Fetch", CopyErrorOrigin.SOURCE, e)

    try:
        data = rc.read()
    finally:
        if hasattr(rc, "close"):
            rc.close()

    _verify_digest(data, desc.get("digest", ""))

    size = desc.get("size", 0)
    if size > 0 and len(data) != size:
        raise CopyError(
            "VerifySize",
            CopyErrorOrigin.SOURCE,
            ValueError(
                f"size mismatch: expected {size}, got {len(data)}"
            ),
        )

    try:
        dst.push(desc, io.BytesIO(data))
    except FileExistsError:
        pass
    except Exception as e:
        raise CopyError("Push", CopyErrorOrigin.DESTINATION, e)


def _mount_or_copy_node(
    src: content_mod.ReadOnlyStorage,
    dst: content_mod.Storage,
    desc: Descriptor,
    opts: CopyGraphOptions,
) -> None:
    """
    Try to mount a blob from another repository, falling back to copy.

    Mounting is only attempted for non-manifest blobs when mount_from
    is provided and the destination supports the Mounter protocol.

    Matches oras-go's mountOrCopyNode.
    """
    # Only attempt mounting for blobs (not manifests) when mount_from is provided
    if opts.mount_from is None or is_manifest(desc):
        _copy_node(src, dst, desc, opts)
        return

    if not isinstance(dst, content_mod.Mounter):
        _copy_node(src, dst, desc, opts)
        return

    source_repositories = opts.mount_from(desc)
    if not source_repositories:
        _copy_node(src, dst, desc, opts)
        return

    class _MountState:
        def __init__(self):
            self.mount_failed: bool = False

    state = _MountState()

    for i, source_repository in enumerate(source_repositories):
        state.mount_failed = False

        def get_content(i=i):
            state.mount_failed = True
            if i < len(source_repositories) - 1:
                # Not the last source; signal to try next
                raise _SkipSource()
            # Last source: actually fetch and copy
            if opts.pre_copy is not None:
                try:
                    opts.pre_copy(desc)
                except Exception as e:
                    if isinstance(e, SkipNode):
                        return io.BytesIO(b"")
                    raise
            return src.fetch(desc)

        try:
            dst.mount(desc, source_repository, get_content)
        except _SkipSource:
            continue
        except Exception as e:
            raise CopyError("Mount", CopyErrorOrigin.DESTINATION, e)

        if not state.mount_failed:
            # Mount succeeded
            if opts.on_mounted is not None:
                opts.on_mounted(desc)
            return

    # Copied via the last get_content fallback
    if opts.post_copy is not None:
        opts.post_copy(desc)


class _SkipSource(Exception):
    """Internal sentinel for skipping to the next mount source."""

    pass
