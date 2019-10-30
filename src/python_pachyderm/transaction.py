from contextlib import contextmanager

from python_pachyderm.proto.transaction import transaction_pb2 as transaction_proto
from python_pachyderm.proto.transaction import transaction_pb2_grpc as transaction_grpc


def transaction_from(transaction):
    if isinstance(transaction, transaction_proto.Transaction):
        return transaction
    else:
        return transaction_proto.Transaction(id=transaction)


class TransactionMixin:
    @property
    def _transaction_stub(self):
        if not hasattr(self, "__transaction_stub"):
            self.__transaction_stub = self._create_stub(transaction_grpc)
        return self.__transaction_stub

    def start_transaction(self):
        """
        Starts a transaction.
        """
        req = transaction_proto.StartTransactionRequest()
        return self._transaction_stub.StartTransaction(req, metadata=self.metadata)

    def inspect_transaction(self, transaction):
        """
        Inspects a given transaction.

        Params:

        * `transaction`: A string or `Transaction` object.
        """
        req = transaction_proto.InspectTransactionRequest(transaction=transaction_from(transaction))
        return self._transaction_stub.InspectTransaction(req, metadata=self.metadata)

    def delete_transaction(self, transaction):
        """
        Deletes a given transaction.

        Params:

        * `transaction`: A string or `Transaction` object.
        """
        req = transaction_proto.DeleteTransactionRequest(transaction=transaction_from(transaction))
        return self._transaction_stub.DeleteTransaction(req, metadata=self.metadata)

    def list_transaction(self):
        """
        Lists transactions.
        """
        req = transaction_proto.ListTransactionRequest()
        return self._transaction_stub.ListTransaction(req, metadata=self.metadata).transaction_info

    def finish_transaction(self, transaction):
        """
        Finishes a given transaction.

        Params:

        * `transaction`: A string or `Transaction` object.
        """
        req = transaction_proto.FinishTransactionRequest(transaction=transaction_from(transaction))
        return self._transaction_stub.FinishTransaction(req, metadata=self.metadata)

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
