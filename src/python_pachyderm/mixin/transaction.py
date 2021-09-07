from contextlib import contextmanager
from typing import Iterator, List, Union

from python_pachyderm.service import Service, transaction_proto


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
        """
        return self._req(Service.TRANSACTION, "BatchTransaction", requests=requests)

    def start_transaction(self) -> transaction_proto.Transaction:
        """Starts a transaction.

        Returns
        -------
        transaction_proto.Transaction
            A protobuf object that represents the transaction.
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
        """Lists transactions.

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
