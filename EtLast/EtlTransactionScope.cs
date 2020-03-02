namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
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
                    Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "new transaction started", newId, null,
                        "new transaction started");
                    break;
                case TransactionScopeKind.Required:
                    if (previousId == null || newId != previousId)
                    {
                        Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "new transaction started", newId, null,
                            "new transaction started");
                    }
                    else
                    {
                        Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "new transaction started and merged with previous", newId, () => new[] { new KeyValuePair<string, object>("previous transaction", previousId) },
                            "new transaction started and merged with previous");
                    }

                    break;
                case TransactionScopeKind.Suppress:
                    if (previousId != null)
                    {
                        Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "existing transaction suppressed", previousId, null,
                            "existing transaction suppressed");
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

            CompleteCalled = true;

            var iocUid = Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, Convert.ToInt32(Timeout.TotalSeconds), "completing transaction", transactionId, null,
                "completing transaction");
            try
            {
                Scope.Complete();
                Completed = true;

                Context.RegisterIoCommandSuccess(Process, iocUid, 0);
            }
            catch (Exception ex)
            {
                Completed = false;
                Context.RegisterIoCommandFailed(Process, iocUid, 0, ex);
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
                            Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "reverting transaction", transactionId, null,
                                "reverting transaction");
                        }

                        Scope.Dispose();
                        Scope = null;

                        if (Kind == TransactionScopeKind.Suppress && !CompleteCalled)
                        {
                            var transactionId = Transaction.Current?.ToIdentifierString();
                            Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "suppression of transaction is removed", transactionId, null,
                                "suppression of transaction is removed");
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
