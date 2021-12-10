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
        call_details_metadata = list(call_details.metadata or [])
        call_details_metadata.extend(self.metadata)
        new_details = ClientCallDetails(
            compression=call_details.compression,
            credentials=call_details.credentials,
            metadata=call_details_metadata,
            method=call_details.method,
            timeout=call_details.timeout,
            wait_for_ready=call_details.wait_for_ready,
        )
        return method(request, new_details)
