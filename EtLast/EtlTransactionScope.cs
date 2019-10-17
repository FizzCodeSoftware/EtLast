namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;
    using System.Transactions;

    public class EtlTransactionScope : IDisposable
    {
        public IEtlContext Context { get; }
        public ICaller Caller { get; }
        public TransactionScopeKind Kind { get; }
        public TransactionScope Scope { get; private set; }
        public LogSeverity LogSeverity { get; }

        public bool CompleteCalled { get; private set; }
        public bool Completed { get; private set; }

        private bool _isDisposed;

        public EtlTransactionScope(IEtlContext context, ICaller caller, TransactionScopeKind kind, TimeSpan scopeTimeout, LogSeverity logSeverity)
        {
            Context = context;
            Caller = caller;
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
                    Context.Log(logSeverity, caller, "new transaction started: {Transaction}", newId);
                    break;
                case TransactionScopeKind.Required:
                    if (previousId == null || newId != previousId)
                    {
                        Context.Log(logSeverity, caller, "new transaction started: {Transaction}", newId);
                    }
                    else
                    {
                        Context.Log(logSeverity, caller, "new transaction started and merged with previous: {Transaction}", newId);
                    }

                    break;
                case TransactionScopeKind.Suppress:
                    if (previousId != null)
                    {
                        Context.Log(logSeverity, caller, "existing transaction suppressed: {Transaction}", previousId);
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
            Context.Log(LogSeverity, Caller, "completing transaction: {Transaction}", transactionId);
            var startedOn = Stopwatch.StartNew();

            CompleteCalled = true;

            try
            {
                Scope.Complete();
                Completed = true;
            }
            catch (Exception ex)
            {
                Context.Log(LogSeverity, Caller, "transaction completition failed after {Elapsed}: {Transaction}, error message: {ExceptionMessage}", startedOn.Elapsed, transactionId, ex.Message);
                Completed = false;
            }

            Context.Log(LogSeverity, Caller, "transaction completed in {Elapsed}: {Transaction}", startedOn.Elapsed, transactionId);
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
                            Context.Log(LogSeverity, Caller, "reverting transaction {Transaction}", transactionId);
                        }

                        Scope.Dispose();
                        Scope = null;

                        if (Kind == TransactionScopeKind.Suppress && !CompleteCalled)
                        {
                            var transactionId = Transaction.Current?.ToIdentifierString();
                            Context.Log(LogSeverity, Caller, "suppression of transaction {Transaction} is removed", transactionId);
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
