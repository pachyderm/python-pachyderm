from contextlib import contextmanager

from python_pachyderm.proto.transaction import transaction_pb2 as transaction_proto
from python_pachyderm.service import Service


def transaction_from(transaction):
    if isinstance(transaction, transaction_proto.Transaction):
        return transaction
    else:
        return transaction_proto.Transaction(id=transaction)


class TransactionMixin:
    def start_transaction(self):
        """
        Starts a transaction.
        """
        return self._req(Service.TRANSACTION, "StartTransaction")

    def inspect_transaction(self, transaction):
        """
        Inspects a given transaction.

        Params:

        * `transaction`: A string or `Transaction` object.
        """
        return self._req(Service.TRANSACTION, "InspectTransaction", transaction=transaction_from(transaction))

    def delete_transaction(self, transaction):
        """
        Deletes a given transaction.

        Params:

        * `transaction`: A string or `Transaction` object.
        """
        return self._req(Service.TRANSACTION, "DeleteTransaction", transaction=transaction_from(transaction))

    def delete_all_transactions(self):
        """
        Deletes all transactions.
        """
        return self._req(Service.TRANSACTION, "DeleteAll")

    def list_transaction(self):
        """
        Lists transactions.
        """
        return self._req(Service.TRANSACTION, "ListTransaction").transaction_info

    def finish_transaction(self, transaction):
        """
        Finishes a given transaction.

        Params:

        * `transaction`: A string or `Transaction` object.
        """
        return self._req(Service.TRANSACTION, "FinishTransaction", transaction=transaction_from(transaction))

    @contextmanager
    def transaction(self, transaction=None):
        """
        A context manager for running operations within a transaction. When
        the context manager completes, the transaction will be deleted if an
        error occurred, or otherwise finished.

        Params:

        * `transaction`: An optional string or `Transaction` object. If
        unspecified, a new transaction will be started.
        """

        # note that this is different from `pachctl`, which will delete any
        # active transaction
        for (k, v) in self.metadata:
            if k == "pach-transaction":
                raise Exception("this client already has an active transaction with ID={}".format(v))

        if transaction is None:
            transaction = self.start_transaction()
        else:
            transaction = transaction_from(transaction)

        self.metadata.append(("pach-transaction", transaction.id))

        try:
            yield transaction
        except Exception:
            self.delete_transaction(transaction)
            raise
        else:
            self.finish_transaction(transaction)
        finally:
            self.metadata = [(k, v) for (k, v) in self.metadata if k != "pach-transaction"]
