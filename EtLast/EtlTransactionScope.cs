namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Transactions;

    public class EtlTransactionScope : IDisposable
    {
        public IEtlContext Context { get; }
        public IProcess Process { get; }
        public TransactionScopeKind Kind { get; }
        public TransactionScope Scope { get; private set; }
        public LogSeverity LogSeverity { get; }

        public bool CompleteCalled { get; private set; }
        public bool Completed { get; private set; }

        private bool _isDisposed;

        public EtlTransactionScope(IEtlContext context, IProcess process, TransactionScopeKind kind, TimeSpan scopeTimeout, LogSeverity logSeverity)
        {
            Context = context;
            Process = process;
            Kind = kind;
            LogSeverity = logSeverity;

            if (Kind == TransactionScopeKind.None)
                return;

            var previousId = Transaction.Current?.ToIdentifierString();

            Scope = new TransactionScope((TransactionScopeOption)Kind, scopeTimeout);

            var newId = Transaction.Current?.ToIdentifierString();

            switch (kind)
            {
                case TransactionScopeKind.RequiresNew:
                    Context.LogNoDiag(logSeverity, Process, "new transaction started: {Transaction}", newId);
                    Context.OnContextDataStoreCommand?.Invoke(DataStoreCommandKind.transaction, null, process, "new transaction started", newId, null);
                    break;
                case TransactionScopeKind.Required:
                    if (previousId == null || newId != previousId)
                    {
                        Context.Log(logSeverity, Process, "new transaction started: {Transaction}", newId);
                        Context.OnContextDataStoreCommand?.Invoke(DataStoreCommandKind.transaction, null, process, "new transaction started", newId, null);
                    }
                    else
                    {
                        Context.Log(logSeverity, Process, "new transaction started and merged with previous: {Transaction}", newId);
                        Context.OnContextDataStoreCommand?.Invoke(DataStoreCommandKind.transaction, null, process, "new transaction started and merged with previous", newId, new[] { new KeyValuePair<string, object>("previous transaction", previousId) });
                    }

                    break;
                case TransactionScopeKind.Suppress:
                    if (previousId != null)
                    {
                        Context.Log(logSeverity, Process, "existing transaction suppressed: {Transaction}", previousId);
                        Context.OnContextDataStoreCommand?.Invoke(DataStoreCommandKind.transaction, null, process, "existing transaction suppressed", previousId, null);
                    }
                    break;
            }
        }

        public void Complete()
        {
            if (Scope == null)
                return;

            if (Kind == TransactionScopeKind.Suppress)
            {
                Scope.Complete();
                return;
            }

            var transactionId = Transaction.Current.ToIdentifierString();
            Context.Log(LogSeverity, Process, "completing transaction: {Transaction}", transactionId);
            Context.OnContextDataStoreCommand?.Invoke(DataStoreCommandKind.transaction, null, Process, "completing transaction", transactionId, null);
            var startedOn = Stopwatch.StartNew();

            CompleteCalled = true;

            try
            {
                Scope.Complete();
                Completed = true;

                Context.Log(LogSeverity, Process, "transaction completed in {Elapsed}: {Transaction}",
                    startedOn.Elapsed, transactionId);

                Context.OnContextDataStoreCommand?.Invoke(DataStoreCommandKind.transaction, null, Process, "transaction completed", transactionId, null);
            }
            catch (Exception ex)
            {
                Completed = false;

                Context.Log(LogSeverity, Process, "transaction completition failed after {Elapsed}: {Transaction}, error message: {ExceptionMessage}", startedOn.Elapsed,
                    transactionId, ex.Message);

                Context.OnContextDataStoreCommand?.Invoke(DataStoreCommandKind.transaction, null, Process, "transaction completition failed", transactionId, new[] { new KeyValuePair<string, object>("error message", ex.Message) });
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    if (Scope != null)
                    {
                        if (Kind != TransactionScopeKind.Suppress && !CompleteCalled)
                        {
                            var transactionId = Transaction.Current?.ToIdentifierString();
                            Context.Log(LogSeverity, Process, "reverting transaction {Transaction}", transactionId);
                            Context.OnContextDataStoreCommand?.Invoke(DataStoreCommandKind.transaction, null, Process, "reverting transaction", transactionId, null);
                        }

                        Scope.Dispose();
                        Scope = null;

                        if (Kind == TransactionScopeKind.Suppress && !CompleteCalled)
                        {
                            var transactionId = Transaction.Current?.ToIdentifierString();
                            Context.Log(LogSeverity, Process, "suppression of transaction {Transaction} is removed", transactionId);
                            Context.OnContextDataStoreCommand?.Invoke(DataStoreCommandKind.transaction, null, Process, "suppression of transaction is removed", transactionId, null);
                        }
                    }
                }

                _isDisposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
