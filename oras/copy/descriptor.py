"""
Descriptor utility functions for OCI content descriptors.

Provides helpers for identifying manifest types, foreign layers,
and creating hashable keys from descriptors for deduplication.
"""

__author__ = "The ORAS Authors"
__license__ = "Apache-2.0"

from typing import Tuple

from oras.types import Descriptor

__all__ = [
    "Descriptor",
    "is_manifest",
    "is_foreign_layer",
    "descriptor_key",
    "descriptors_equal",
    "remove_foreign_layers",
]

# OCI and Docker manifest media types
_MANIFEST_MEDIA_TYPES = frozenset(
    {
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
    }
)

# Non-distributable / foreign layer media types
_FOREIGN_LAYER_MEDIA_TYPES = frozenset(
    {
        "application/vnd.oci.image.layer.nondistributable.v1.tar",
        "application/vnd.oci.image.layer.nondistributable.v1.tar+gzip",
        "application/vnd.oci.image.layer.nondistributable.v1.tar+zstd",
        "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip",
    }
)


def is_manifest(desc: Descriptor) -> bool:
    """Check if a descriptor refers to a manifest or index."""
    return desc.get("mediaType", "") in _MANIFEST_MEDIA_TYPES


def is_foreign_layer(desc: Descriptor) -> bool:
    """Check if a descriptor refers to a non-distributable foreign layer."""
    return desc.get("mediaType", "") in _FOREIGN_LAYER_MEDIA_TYPES


def descriptor_key(desc: Descriptor) -> Tuple[str, str, int]:
    """
    Create a hashable key from a descriptor for deduplication.

    Uses the minimal identity triple: (mediaType, digest, size).
    """
    return (
        desc.get("mediaType", ""),
        desc.get("digest", ""),
        desc.get("size", 0),
    )


def descriptors_equal(a: Descriptor, b: Descriptor) -> bool:
    """Check if two descriptors refer to the same content."""
    return descriptor_key(a) == descriptor_key(b)


def remove_foreign_layers(descs: list) -> list:
    """Filter out non-distributable foreign layers from a descriptor list."""
    return [d for d in descs if not is_foreign_layer(d)]
