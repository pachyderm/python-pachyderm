import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc


class HealthMixin:
    """A mixin for health-related functionality."""

    _channel: grpc.Channel

    def __init__(self):
        self.__stub = health_pb2_grpc.HealthStub(self._channel)
        super().__init__()

    def health_check(self) -> health_pb2.HealthCheckResponse:
        """Returns a health check indicating if the server can handle
        RPCs.

        Returns
        -------
        health_pb2HealthCheckResponse
            A protobuf object with a status enum indicating server health.
        """
        message = health_pb2.HealthCheckRequest()
        return self.__stub.Check(message)
