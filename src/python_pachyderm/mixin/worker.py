from contextlib import contextmanager
from typing import ContextManager, Optional

import grpc
from dotenv import load_dotenv

from python_pachyderm.proto.v2.worker import worker_pb2, worker_pb2_grpc


class WorkerMixin:
    """A mixin for worker binary-related functionality."""

    _dotenv_path = "/pfs/.env"
    __error: Optional[str] = None  # used by batch_datum

    def __init__(self, channel: grpc.Channel):
        """
        Note: This API stub must have an open grpc channel work the worker
        binary. The ``Client`` object should automatically do this for
        the user.
        """
        self.__stub = worker_pb2_grpc.WorkerStub(channel)
        super().__init__()

    def next_datum(self, *, error: str = "") -> worker_pb2.NextDatumResponse:
        """Calls the NextDatum endpoint within the worker to step forward during
        datum batching.

        Parameters
        ----------
        error : str, optional
            An error message to report to the worker binary indicating the
            current datum could not be processed.
        """
        message = worker_pb2.NextDatumRequest(error=error)
        return self.__stub.NextDatum(message)

    @contextmanager
    def batch_datum(self) -> ContextManager:
        """A ContextManager that, when entered, calls the NextDatum
        endpoint within the worker to step forward during datum batching.
        This context manager will also prepare the environment for the user
        and report errors that occur back to the worker.

        This context manager expects to be called within an infinite while
        loop -- see the examples section. This context can only be entered
        from within a Pachyderm worker and the worker will terminate your
        code when all datums have been processed.

        Note: The API stub must have an open gRPC channel with the worker
        for NextDatum to function correctly. The ``Client`` object
        should automatically do this for the user.

        Examples
        --------
        >>> from python_pachyderm import Client
        >>>
        >>> worker = Client().worker
        >>>
        >>> # Perform an expensive computation here before
        >>> #   you being processing your datums
        >>> #   i.e. initializing a model.
        >>>
        >>> while True:
        >>>     with worker.batch_datum():
        >>>         # process datums
        >>>         pass
        """
        self.next_datum(error=self.__error or "")
        load_dotenv(self._dotenv_path, override=True)

        self.__error = None
        try:
            yield
        except Exception as error:
            self.__error = repr(error)
            # TODO: Probably want better logging here than a print statement.
            print(f"{self.__error}\nReporting above error to worker.")
