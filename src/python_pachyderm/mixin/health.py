from python_pachyderm.service import health_proto, Service


class HealthMixin:
    def health_check(self):
        return self._req(
            Service.HEALTH,
            "Check",
            req=health_proto.HealthCheckRequest(),
        )
