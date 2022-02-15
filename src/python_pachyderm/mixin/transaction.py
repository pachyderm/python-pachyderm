from contextlib import contextmanager
from typing import Iterator, List, Union

import grpc

from python_pachyderm.proto.v2.transaction import transaction_pb2, transaction_pb2_grpc


def _transaction_from(transaction):
    if isinstance(transaction, transaction_pb2.Transaction):
        return transaction
    else:
        return transaction_pb2.Transaction(id=transaction)


class TransactionMixin:
    """A mixin for transaction-related functionality."""

    _channel: grpc.Channel

    def __init__(self):
        self.__stub = transaction_pb2_grpc.APIStub(self._channel)
        super().__init__()

    def batch_transaction(
        self, requests: List[transaction_pb2.TransactionRequest]
    ) -> transaction_pb2.TransactionInfo:
        """Executes a batch transaction.

        Parameters
        ----------
        requests : List[transaction_pb2.TransactionRequest]
            A list of ``TransactionRequest`` protobufs. Each protobuf must
            only have one field set.

        Returns
        -------
        transaction_pb2.TransactionInfo
            A protobuf object with info on the transaction.

        Examples
        --------
        >>> # Deletes one repo and creates a branch in another repo atomically
        >>> client.batch_transaction([
            transaction_pb2.TransactionRequest(delete_repo=pfs_pb2.DeleteRepoRequest(repo=pfs_pb2.Repo(name="foo"))),
            transaction_pb2.TransactionRequest(create_branch=pfs_pb2.CreateBranchRequest(branch=pfs_pb2.Branch(
                repo=pfs_pb2.Repo(name="bar", type="user"), name="staging"
            )))
        ])
        """
        message = transaction_pb2.BatchTransactionRequest(requests=requests)
        return self.__stub.BatchTransaction(message)

    def start_transaction(self) -> transaction_pb2.Transaction:
        """Starts a transaction.

        Returns
        -------
        transaction_pb2.Transaction
            A protobuf object that represents the transaction.

        Examples
        --------
        >>> transaction = client.start_transaction()
        >>> # do stuff
        >>> client.finish_transaction(transaction)
        """
        message = transaction_pb2.StartTransactionRequest()
        return self.__stub.StartTransaction(message)

    def inspect_transaction(
        self, transaction: Union[str, transaction_pb2.Transaction]
    ) -> transaction_pb2.TransactionInfo:
        """Inspects a transaction.

        Parameters
        ----------
        transaction : Union[str, transaction_pb2.Transaction]
            The ID or protobuf object representing the transaction.

        Returns
        -------
        transaction_pb2.TransactionInfo
            A protobuf object with info on the transaction.

        Examples
        --------
        >>> transaction = client.inspect_transaction("6fe754facd9c41e99d04e1037e3be9ee")
        ...
        >>> transaction = client.inspect_transaction(transaction_protobuf)

        .. # noqa: W505
        """
        message = transaction_pb2.InspectTransactionRequest(
            transaction=_transaction_from(transaction),
        )
        return self.__stub.InspectTransaction(message)

    def delete_transaction(
        self, transaction: Union[str, transaction_pb2.Transaction]
    ) -> None:
        """Deletes a transaction.

        Parameters
        ----------
        transaction : Union[str, transaction_pb2.Transaction]
            The ID or protobuf object representing the transaction.

        Examples
        --------
        >>> client.delete_transaction("6fe754facd9c41e99d04e1037e3be9ee")
        ...
        >>> transaction = client.finish_transaction("a3ak09467c580611234cdb8cc9758c7a")
        >>> client.delete_transaction(transaction)

        .. # noqa: W505
        """
        message = transaction_pb2.DeleteTransactionRequest(
            transaction=_transaction_from(transaction),
        )
        self.__stub.DeleteTransaction(message)

    def delete_all_transactions(self) -> None:
        """Deletes all transactions."""
        message = transaction_pb2.DeleteAllRequest()
        self.__stub.DeleteAll(message)

    def list_transaction(self) -> List[transaction_pb2.TransactionInfo]:
        """Lists unfinished transactions.

        Returns
        -------
        List[transaction_pb2.TransactionInfo]
            A list of protobuf objects that contain info on a transaction.
        """
        message = transaction_pb2.ListTransactionRequest()
        return self.__stub.ListTransaction(message).transaction_info

    def finish_transaction(
        self, transaction: Union[str, transaction_pb2.Transaction]
    ) -> transaction_pb2.TransactionInfo:
        """Finishes a transaction.

        Parameters
        ----------
        transaction : Union[str, transaction_pb2.Transaction]
            The ID or protobuf object representing the transaction.

        Returns
        -------
        transaction_pb2.TransactionInfo
            A protobuf object with info on the transaction.

        Examples
        --------
        >>> transaction = client.start_transaction()
        >>> # do stuff
        >>> client.finish_transaction(transaction)
        """
        message = transaction_pb2.FinishTransactionRequest(
            transaction=_transaction_from(transaction)
        )
        return self.__stub.FinishTransaction(message)

    @contextmanager
    def transaction(self) -> Iterator[transaction_pb2.Transaction]:
        """A context manager for running operations within a transaction. When
        the context manager completes, the transaction will be deleted if an
        error occurred, or otherwise finished.

        Yields
        -------
        transaction_pb2.Transaction
            A protobuf object that represents a transaction.

        Examples
        --------
        If a pipeline has two input repos, `foo` and `bar`, a transaction is
        useful for adding data to both atomically before the pipeline runs
        even once.

        >>> with client.transaction() as t:
        >>>     c1 = client.start_commit("foo", "master")
        >>>     c2 = client.start_commit("bar", "master")
        >>>
        >>>     client.put_file_bytes(c1, "/joint_data.txt", b"DATA1")
        >>>     client.put_file_bytes(c2, "/joint_data.txt", b"DATA2")
        >>>
        >>>     client.finish_commit(c1)
        >>>     client.finish_commit(c2)
        """

        old_transaction_id = self.transaction_id
        transaction = self.start_transaction()
        self.transaction_id = transaction.id

        try:
            yield transaction
        except Exception:
            self.delete_transaction(transaction)
            raise
        else:
            self.finish_transaction(transaction)
        finally:
            self.transaction_id = old_transaction_id
