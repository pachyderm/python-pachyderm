from os import environ
from typing import Any, Callable, List, Tuple

import grpc
from grpc_interceptor import ClientCallDetails, ClientInterceptor

MetadataType = List[Tuple[str, str]]


class MetadataClientInterceptor(ClientInterceptor):
    def __init__(self, metadata: MetadataType):
        self.metadata = metadata

    def intercept(
        self, method: Callable, request: Any, call_details: ClientCallDetails
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

        future = method(request, new_details)
        try:
            future.result()
            return future
        except grpc.RpcError as error:
            unable_to_connect = "failed to connect to all addresses" in error.details()
            if error.code() == grpc.StatusCode.UNAVAILABLE and unable_to_connect:
                error_message = "Could not connect to pachyderm instance\n"
                if "PACHD_PEER_SERVICE_HOST" in environ:
                    error_message += (
                        "\tPACHD_PEER_SERVICE_HOST is detected. "
                        "Please use Client.new_in_cluster() when creating when using"
                        " python_pachyderm within the a pipeline. "
                    )
                raise ConnectionError(error_message) from error
            raise error
