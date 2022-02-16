from contextlib import contextmanager
from typing import Iterator, List, Optional, Union

from ..proto.v2.transaction_v2 import (
    ApiStub as _TransactionApiStub,
    Transaction,
    TransactionInfo,
    TransactionRequest,
)
from . import _synchronizer

TRANSACTION_KEY = "pach-transaction"
TransactionLike = Union[str, Transaction]


def _transaction_from(transaction: TransactionLike):
    if isinstance(transaction, Transaction):
        return transaction
    else:
        return Transaction(id=transaction)


@_synchronizer
class TransactionApi(_synchronizer(_TransactionApiStub)):
    """A mixin for transaction-related functionality."""

    @property
    def id(self) -> Optional[str]:
        return self.metadata.get(TRANSACTION_KEY)

    async def batch(self, requests: List[TransactionRequest]) -> TransactionInfo:
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
        return await super().batch_transaction(requests=requests)

    async def start(self) -> Transaction:
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
        return await super().start_transaction()

    async def inspect(self, transaction: TransactionLike) -> TransactionInfo:
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
        transaction = _transaction_from(transaction)
        return await super().inspect_transaction(transaction=transaction)

    async def delete(self, transaction: TransactionLike) -> None:
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
        transaction = _transaction_from(transaction)
        await super().delete_transaction(transaction=transaction)

    async def delete_all(self) -> None:
        """Deletes all transactions."""
        await super().delete_all()

    async def list(self) -> List[TransactionInfo]:
        """Lists unfinished transactions.

        Returns
        -------
        List[transaction_proto.TransactionInfo]
            A list of protobuf objects that contain info on a transaction.
        """
        response = await super().list_transaction()
        return response.transaction_info

    async def finish(self, transaction: TransactionLike) -> TransactionInfo:
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
        transaction = _transaction_from(transaction)
        return await super().finish_transaction(transaction=transaction)

    @contextmanager
    def transaction(self) -> Iterator[Transaction]:
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
        old_transaction_id = self.metadata.get(TRANSACTION_KEY)
        transaction = self.start_transaction()
        self.metadata[TRANSACTION_KEY] = transaction.id

        try:
            yield transaction
        except Exception:
            self.delete(transaction)
            raise
        else:
            self.finish(transaction)
        finally:
            del self.metadata[TRANSACTION_KEY]
            if old_transaction_id:
                self.metadata[TRANSACTION_KEY] = old_transaction_id
