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
        public TimeSpan Timeout { get; }

        public bool CompleteCalled { get; private set; }
        public bool Completed { get; private set; }

        private bool _isDisposed;

        public EtlTransactionScope(IEtlContext context, IProcess process, TransactionScopeKind kind, TimeSpan scopeTimeout, LogSeverity logSeverity)
        {
            Context = context;
            Process = process;
            Kind = kind;
            LogSeverity = logSeverity;
            Timeout = scopeTimeout;

            if (Kind == TransactionScopeKind.None)
                return;

            var previousId = Transaction.Current?.ToIdentifierString();

            Scope = new TransactionScope((TransactionScopeOption)Kind, scopeTimeout);

            var newId = Transaction.Current?.ToIdentifierString();

            switch (kind)
            {
                case TransactionScopeKind.RequiresNew:
                    Context.Log(newId, logSeverity, Process, "new transaction started");
                    Context.RegisterDataStoreCommandStart(Process, DataStoreCommandKind.transaction, null, null, "new transaction started", newId, null);
                    break;
                case TransactionScopeKind.Required:
                    if (previousId == null || newId != previousId)
                    {
                        Context.Log(newId, logSeverity, Process, "new transaction started");
                        Context.RegisterDataStoreCommandStart(Process, DataStoreCommandKind.transaction, null, null, "new transaction started", newId, null);
                    }
                    else
                    {
                        Context.Log(newId, logSeverity, Process, "new transaction started and merged with previous");
                        Context.RegisterDataStoreCommandStart(Process, DataStoreCommandKind.transaction, null, null, "new transaction started and merged with previous", newId, () => new[] { new KeyValuePair<string, object>("previous transaction", previousId) });
                    }

                    break;
                case TransactionScopeKind.Suppress:
                    if (previousId != null)
                    {
                        Context.Log(previousId, logSeverity, Process, "existing transaction suppressed");
                        Context.RegisterDataStoreCommandStart(Process, DataStoreCommandKind.transaction, null, null, "existing transaction suppressed", previousId, null);
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
            Context.Log(transactionId, LogSeverity, Process, "completing transaction");
            Context.RegisterDataStoreCommandStart(Process, DataStoreCommandKind.transaction, null, Convert.ToInt32(Timeout.TotalSeconds), "completing transaction", transactionId, null);
            var startedOn = Stopwatch.StartNew();

            CompleteCalled = true;

            var dscUid = Context.RegisterDataStoreCommandStart(Process, DataStoreCommandKind.transaction, null, Convert.ToInt32(Timeout.TotalSeconds), "transaction completed", transactionId, null);
            try
            {
                Scope.Complete();
                Completed = true;

                Context.Log(transactionId, LogSeverity, Process, "transaction completed in {Elapsed}",
                    startedOn.Elapsed);

                Context.RegisterDataStoreCommandEnd(Process, dscUid, 0, null);
            }
            catch (Exception ex)
            {
                Completed = false;

                Context.Log(transactionId, LogSeverity.Warning, Process, "transaction completition failed after {Elapsed}, error message: {ExceptionMessage}",
                    startedOn.Elapsed, ex.Message);

                Context.RegisterDataStoreCommandEnd(Process, dscUid, 0, ex.Message);
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
                            Context.Log(transactionId, LogSeverity, Process, "reverting transaction");
                            Context.RegisterDataStoreCommandStart(Process, DataStoreCommandKind.transaction, null, null, "reverting transaction", transactionId, null);
                        }

                        Scope.Dispose();
                        Scope = null;

                        if (Kind == TransactionScopeKind.Suppress && !CompleteCalled)
                        {
                            var transactionId = Transaction.Current?.ToIdentifierString();
                            Context.Log(transactionId, LogSeverity, Process, "suppression of transaction is removed");
                            Context.RegisterDataStoreCommandStart(Process, DataStoreCommandKind.transaction, null, null, "suppression of transaction is removed", transactionId, null);
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
