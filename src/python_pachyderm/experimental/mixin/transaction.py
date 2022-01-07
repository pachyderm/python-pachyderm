from contextlib import contextmanager
from typing import Iterator, List, Union

from python_pachyderm.service import Service
from python_pachyderm.experimental.service import transaction_proto


def _transaction_from(transaction):
    if isinstance(transaction, transaction_proto.Transaction):
        return transaction
    else:
        return transaction_proto.Transaction(id=transaction)


class TransactionMixin:
    """A mixin for transaction-related functionality."""

    def batch_transaction(
        self, requests: List[transaction_proto.TransactionRequest]
    ) -> transaction_proto.TransactionInfo:
        """Executes a batch transaction.

        Parameters
        ----------
        requests : List[transaction_proto.TransactionRequest]
            A list of ``TransactionRequest`` protobufs. Each protobuf must
            only have one field set.

        Returns
        -------
        transaction_proto.TransactionInfo
            A protobuf object with info on the transaction.

        Examples
        --------
        >>> # Deletes one repo and creates a branch in another repo atomically
        >>> client.batch_transaction([
            transaction_proto.TransactionRequest(delete_repo=pfs_proto.DeleteRepoRequest(repo=pfs_proto.Repo(name="foo"))),
            transaction_proto.TransactionRequest(create_branch=pfs_proto.CreateBranchRequest(branch=pfs_proto.Branch(
                repo=pfs_proto.Repo(name="bar", type="user"), name="staging"
            )))
        ])
        """
        return self._req(Service.TRANSACTION, "BatchTransaction", requests=requests)

    def start_transaction(self) -> transaction_proto.Transaction:
        """Starts a transaction.

        Returns
        -------
        transaction_proto.Transaction
            A protobuf object that represents the transaction.

        Examples
        --------
        >>> transaction = client.start_transaction()
        >>> # do stuff
        >>> client.finish_transaction(transaction)
        """
        return self._req(Service.TRANSACTION, "StartTransaction")

    def inspect_transaction(
        self, transaction: Union[str, transaction_proto.Transaction]
    ) -> transaction_proto.TransactionInfo:
        """Inspects a transaction.

        Parameters
        ----------
        transaction : Union[str, transaction_proto.Transaction]
            The ID or protobuf object representing the transaction.

        Returns
        -------
        transaction_proto.TransactionInfo
            A protobuf object with info on the transaction.

        Examples
        --------
        >>> transaction = client.inspect_transaction("6fe754facd9c41e99d04e1037e3be9ee")
        ...
        >>> transaction = client.inspect_transaction(transaction_protobuf)

        .. # noqa: W505
        """
        return self._req(
            Service.TRANSACTION,
            "InspectTransaction",
            transaction=_transaction_from(transaction),
        )

    def delete_transaction(
        self, transaction: Union[str, transaction_proto.Transaction]
    ) -> None:
        """Deletes a transaction.

        Parameters
        ----------
        transaction : Union[str, transaction_proto.Transaction]
            The ID or protobuf object representing the transaction.

        Examples
        --------
        >>> client.delete_transaction("6fe754facd9c41e99d04e1037e3be9ee")
        ...
        >>> transaction = client.finish_transaction("a3ak09467c580611234cdb8cc9758c7a")
        >>> client.delete_transaction(transaction)

        .. # noqa: W505
        """
        self._req(
            Service.TRANSACTION,
            "DeleteTransaction",
            transaction=_transaction_from(transaction),
        )

    def delete_all_transactions(self) -> None:
        """Deletes all transactions."""
        self._req(Service.TRANSACTION, "DeleteAll")

    def list_transaction(self) -> List[transaction_proto.TransactionInfo]:
        """Lists unfinished transactions.

        Returns
        -------
        List[transaction_proto.TransactionInfo]
            A list of protobuf objects that contain info on a transaction.
        """
        return self._req(Service.TRANSACTION, "ListTransaction").transaction_info

    def finish_transaction(
        self, transaction: Union[str, transaction_proto.Transaction]
    ) -> transaction_proto.TransactionInfo:
        """Finishes a transaction.

        Parameters
        ----------
        transaction : Union[str, transaction_proto.Transaction]
            The ID or protobuf object representing the transaction.

        Returns
        -------
        transaction_proto.TransactionInfo
            A protobuf object with info on the transaction.

        Examples
        --------
        >>> transaction = client.start_transaction()
        >>> # do stuff
        >>> client.finish_transaction(transaction)
        """
        return self._req(
            Service.TRANSACTION,
            "FinishTransaction",
            transaction=_transaction_from(transaction),
        )

    @contextmanager
    def transaction(self) -> Iterator[transaction_proto.Transaction]:
        """A context manager for running operations within a transaction. When
        the context manager completes, the transaction will be deleted if an
        error occurred, or otherwise finished.

        Yields
        -------
        transaction_proto.Transaction
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
