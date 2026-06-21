"""
OCI content layer for the copy engine.

Holds the storage/target Protocol contract (:mod:`oras.content.storage`) and
the in-memory store + read-through cache (:mod:`oras.content.memory`) shared by
the copy algorithm and the concrete target adapters.
"""

__author__ = "The ORAS Authors"
__copyright__ = "Copyright The ORAS Authors."
__license__ = "Apache-2.0"
