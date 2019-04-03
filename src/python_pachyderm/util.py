import os

def get_address(host=None, port=None):
    if host is None or port is None:
        env_old_host = os.environ.get("PACHD_SERVICE_HOST")

        if env_old_host is not None:
            import warnings
            warnings.warn("The environment variable `PACHD_SERVICE_HOST` is deprecated; either explicitly set the host, or use `PACHD_ADDRESS`", DeprecationWarning)
            host = env_old_host

    if port is None:
        env_old_port = os.environ.get("PACHD_SERVICE_PORT_API_GRPC_PORT")

        if env_old_port is not None:
            import warnings
            warnings.warn("The environment variable `PACHD_SERVICE_PORT_API_GRPC_PORT` is deprecated; either explicitly set the host, or use `PACHD_ADDRESS`", DeprecationWarning)
            port = env_old_port

    if host is not None and port is not None:
        return "{}:{}".format(host, port)
    else:
        return os.environ.get("PACHD_ADDRESS", "localhost:30650")
