from typing import Any, Callable, List, Tuple

import grpc
from grpc_interceptor import ClientCallDetails, ClientInterceptor

MetadataType = List[Tuple[str, str]]


class MetadataClientInterceptor(ClientInterceptor):
    def __init__(self, metadata: MetadataType):
        self.metadata = metadata

    def intercept(
        self, method: Callable, request: Any, call_details: grpc.ClientCallDetails
    ):
        old_metadata = list(call_details.metadata or [])
        new_details = ClientCallDetails(
            call_details.method,
            call_details.timeout,
            old_metadata.extend(self.metadata),
            call_details.credentials,
            call_details.wait_for_ready,
            call_details.compression,
        )
        return method(request, new_details)
