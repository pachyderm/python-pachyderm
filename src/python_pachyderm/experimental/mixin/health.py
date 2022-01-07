from python_pachyderm.service import Service
from python_pachyderm.experimental.service import health_proto


class HealthMixin:
    """A mixin for health-related functionality."""

    def health_check(self) -> health_proto.HealthCheckResponse:
        """Returns a health check indicating if the server can handle
        RPCs.

        Returns
        -------
        health_proto.HealthCheckResponse
            A protobuf object with a status enum indicating server health.
        """
        return self._req(
            Service.HEALTH,
            "Check",
            req=health_proto.HealthCheckRequest(),
        )
