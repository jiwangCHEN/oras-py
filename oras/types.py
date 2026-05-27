__author__ = "Vanessa Sochat"
__copyright__ = "Copyright The ORAS Authors."
__license__ = "Apache-2.0"

from typing import Any, Dict, Union

import oras.container

# container type can be string or container
container_type = Union[str, oras.container.Container]

# OCI content descriptor (mediaType + digest + size, plus optional fields)
Descriptor = Dict[str, Any]
