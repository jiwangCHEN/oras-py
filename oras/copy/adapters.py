"""
Adapter classes for the copy engine.

Bridges existing oras.provider.Registry and oras.layout.Layout to the
copy engine's Protocol interfaces (Target, ReadOnlyTarget, etc.).

RegistryTarget wraps a Registry + container string into a Target +
ReferencePusher + Mounter scoped to a single repository.

LayoutTarget wraps a Layout directory into a ReadOnlyTarget.
"""

__author__ = "The ORAS Authors"
__license__ = "Apache-2.0"

import io
import os
import pathlib
import re
import tempfile
from typing import BinaryIO, Callable

import oras.defaults
import oras.utils
from oras.copy.descriptor import Descriptor, is_manifest
from oras.layout.layout import Layout
from oras.provider import Registry
from oras.utils.fileio import read_json

_VALID_DIGEST_RE = re.compile(r"^[a-z0-9]+:[a-f0-9]+$")

# Broad Accept header covering all manifest media types for resolve()
_ACCEPT_ALL_MANIFESTS = ", ".join(
    [
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
    ]
)


class RegistryTarget:
    """
    Adapts a Registry + container string into the copy engine's
    Target, ReferencePusher, and Mounter protocols.

    All operations are scoped to the single repository identified
    by the container string passed at construction time.
    """

    def __init__(self, registry: Registry, container: str):
        self._registry = registry
        self._container = registry.get_container(container)
        self._registry.auth.load_configs(self._container)

    def _manifest_url(self, ref: str) -> str:
        """Build full manifest URL for a reference (tag or digest)."""
        return f"{self._registry.prefix}://{self._container.manifest_url(ref)}"

    def _upload_blob_url(self) -> str:
        """Build full upload blob URL."""
        return f"{self._registry.prefix}://{self._container.upload_blob_url()}"

    def fetch(self, desc: Descriptor) -> BinaryIO:
        """Fetch content for a descriptor. Routes blob vs manifest."""
        if is_manifest(desc):
            url = self._manifest_url(desc["digest"])
            headers = {"Accept": desc.get("mediaType", "")}
            response = self._registry.do_request(url, "GET", headers=headers)
            self._registry._check_200_response(response)
            return io.BytesIO(response.content)
        else:
            response = self._registry.get_blob(self._container, desc["digest"])
            self._registry._check_200_response(response)
            return io.BytesIO(response.content)

    def exists(self, desc: Descriptor) -> bool:
        """Check if content exists. Routes blob vs manifest."""
        if is_manifest(desc):
            url = self._manifest_url(desc["digest"])
            response = self._registry.do_request(url, "HEAD")
            return response.status_code == 200
        else:
            return self._registry.blob_exists(desc, self._container)

    def push(self, desc: Descriptor, content: BinaryIO) -> None:
        """Push content for a descriptor. Routes blob vs manifest."""
        if is_manifest(desc):
            data = content.read()
            url = self._manifest_url(desc["digest"])
            headers = {"Content-Type": desc.get("mediaType", "")}
            response = self._registry.do_request(
                url, "PUT", data=data, headers=headers
            )
            self._registry._check_200_response(response)
        else:
            data = content.read()
            tmp = None
            try:
                tmp = tempfile.NamedTemporaryFile(delete=False)
                tmp.write(data)
                tmp.close()
                self._registry.upload_blob(tmp.name, self._container, desc)
            finally:
                if tmp is not None:
                    try:
                        os.unlink(tmp.name)
                    except OSError:
                        pass

    def tag(self, desc: Descriptor, reference: str) -> None:
        """Tag a descriptor with a reference by re-uploading the manifest."""
        rc = self.fetch(desc)
        data = rc.read()
        url = self._manifest_url(reference)
        headers = {"Content-Type": desc.get("mediaType", "")}
        response = self._registry.do_request(
            url, "PUT", data=data, headers=headers
        )
        self._registry._check_200_response(response)

    def resolve(self, reference: str) -> Descriptor:
        """Resolve a reference to a descriptor via HEAD request."""
        url = self._manifest_url(reference)
        headers = {"Accept": _ACCEPT_ALL_MANIFESTS}
        response = self._registry.do_request(url, "HEAD", headers=headers)
        self._registry._check_200_response(response)
        return {
            "mediaType": response.headers.get("Content-Type", ""),
            "digest": response.headers.get("Docker-Content-Digest", ""),
            "size": int(response.headers.get("Content-Length", 0)),
        }

    def push_reference(
        self, desc: Descriptor, content: BinaryIO, reference: str
    ) -> None:
        """Atomic push + tag: PUT manifest bytes to the reference URL."""
        data = content.read()
        url = self._manifest_url(reference)
        headers = {"Content-Type": desc.get("mediaType", "")}
        response = self._registry.do_request(
            url, "PUT", data=data, headers=headers
        )
        self._registry._check_200_response(response)

    def mount(
        self,
        desc: Descriptor,
        from_repo: str,
        get_content: Callable[[], BinaryIO],
    ) -> None:
        """
        Attempt cross-repo blob mount, falling back to regular upload.

        POSTs to the blob upload endpoint with mount and from params.
        If the registry returns 201, the mount succeeded. If 202, the
        mount failed and we complete the upload session with get_content().
        """
        url = oras.utils.append_url_params(
            self._upload_blob_url(),
            {"mount": desc["digest"], "from": from_repo},
        )
        response = self._registry.do_request(url, "POST")
        if response.status_code == 201:
            return  # Mount succeeded

        # Mount failed (202) — fall back to regular upload
        content = get_content()
        data = content.read()
        session_url = self._registry._get_location(response, self._container)
        if not session_url:
            raise ValueError("Mount fallback: no session URL in response")

        blob_url = oras.utils.append_url_params(
            session_url, {"digest": desc["digest"]}
        )
        headers = {
            "Content-Length": str(len(data)),
            "Content-Type": "application/octet-stream",
        }
        response = self._registry.do_request(
            blob_url, "PUT", data=data, headers=headers
        )
        self._registry._check_200_response(response)


class LayoutTarget:
    """
    Adapts a Layout directory into the copy engine's ReadOnlyTarget protocol.

    Provides read-only access to the OCI layout's content-addressable blobs
    and resolves references via the layout's index.json annotations.
    """

    def __init__(self, layout: Layout):
        self._layout = layout

    @staticmethod
    def _validate_digest(digest: str) -> None:
        if not _VALID_DIGEST_RE.match(digest):
            raise ValueError(f"invalid digest format: {digest!r}")

    def fetch(self, desc: Descriptor) -> BinaryIO:
        """Fetch blob content by digest, returning an open file handle."""
        digest = desc["digest"]
        self._validate_digest(digest)
        path = self._layout.digest_to_blob_path(digest)
        return open(path, "rb")

    def exists(self, desc: Descriptor) -> bool:
        """Check if a blob exists on disk."""
        digest = desc["digest"]
        self._validate_digest(digest)
        path = self._layout.digest_to_blob_path(digest)
        return path.exists()

    def resolve(self, reference: str) -> Descriptor:
        """Resolve a reference tag to a descriptor via index.json."""
        layout_dir = pathlib.Path(self._layout._oci_layout_path)
        index_path = layout_dir / oras.defaults.oci_image_index_file
        index_data = read_json(str(index_path))
        for entry in index_data.get("manifests", []):
            annotations = entry.get("annotations", {})
            if (
                annotations.get(oras.defaults.oci_ref_name_annotation)
                == reference
            ):
                return {
                    "mediaType": entry.get("mediaType", ""),
                    "digest": entry.get("digest", ""),
                    "size": entry.get("size", 0),
                }
        raise FileNotFoundError(
            f"Reference not found in layout index: {reference}"
        )
