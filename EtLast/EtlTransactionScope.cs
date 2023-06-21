namespace FizzCode.EtLast;

public sealed class EtlTransactionScope : IDisposable
{
    public IEtlContext Context { get; }
    public IProcess Process { get; }
    public TransactionScopeKind Kind { get; }
    public LogSeverity LogSeverity { get; }
    public TimeSpan Timeout { get; }

    public bool CompleteCalled { get; private set; }
    private string completitionTransactionId;

    private TransactionScope _scope;
    private bool _isDisposed;

    public EtlTransactionScope(IEtlContext context, IProcess process, TransactionScopeKind kind, TimeSpan scopeTimeout, LogSeverity logSeverity = LogSeverity.Information)
    {
        Context = context;
        Process = process;
        Kind = kind;
        LogSeverity = logSeverity;
        Timeout = scopeTimeout;

        if (Kind == TransactionScopeKind.None)
            return;

        var previousId = Transaction.Current?.ToIdentifierString();

        if (Kind == TransactionScopeKind.Suppress && previousId == null)
            return;

        _scope = new TransactionScope((TransactionScopeOption)Kind, scopeTimeout);

        var newId = Transaction.Current?.ToIdentifierString();

        var iocUid = 0;
        switch (kind)
        {
            case TransactionScopeKind.RequiresNew:
                if (logSeverity != LogSeverity.Verbose)
                    Context.Log(logSeverity, Process, "new transaction started");

                iocUid = Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "new transaction started", newId, null,
                    "new transaction started");
                break;
            case TransactionScopeKind.Required:
                if (logSeverity != LogSeverity.Verbose)
                    Context.Log(logSeverity, Process, "new transaction started" + (previousId != null && newId == previousId ? " and merged with previous" : ""));

                iocUid = previousId == null || newId != previousId
                    ? Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "new transaction started", newId, null,
                        "new transaction started")
                    : Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "new transaction started and merged with previous", newId, () => new[] { new KeyValuePair<string, object>("previous transaction", previousId) },
                        "new transaction started and merged with previous");
                break;
            case TransactionScopeKind.Suppress:
                if (logSeverity != LogSeverity.Verbose)
                    Context.Log(logSeverity, Process, "existing transaction suppressed");

                iocUid = Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "transaction suppressed", previousId, null,
                    "existing transaction suppressed");
                break;
        }

        Context.RegisterIoCommandSuccess(Process, IoCommandKind.dbTransaction, iocUid, null);
    }

    public void Complete()
    {
        if (_scope == null)
            return;

        if (Kind == TransactionScopeKind.Suppress)
        {
            _scope.Complete();
            return;
        }

        completitionTransactionId = Transaction.Current.ToIdentifierString();

        CompleteCalled = true;
        _scope.Complete();
    }

    public void Dispose(bool disposing)
    {
        if (!_isDisposed)
        {
            if (disposing)
            {
                if (_scope != null)
                {
                    if (!CompleteCalled)
                    {
                        if (Kind != TransactionScopeKind.Suppress)
                        {
                            var transactionId = Transaction.Current?.ToIdentifierString();
                            var iocUid = Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "rollback", transactionId, null,
                                "reverting transaction");

                            if (LogSeverity != LogSeverity.Verbose)
                                Context.Log(LogSeverity, Process, "reverting transaction");

                            try
                            {
                                _scope.Dispose();
                                _scope = null;

                                Context.RegisterIoCommandSuccess(Process, IoCommandKind.dbTransaction, iocUid, null);
                                Context.Log(LogSeverity, Process, "transaction reverted");
                            }
                            catch (Exception ex)
                            {
                                _scope = null;
                                Context.RegisterIoCommandFailed(Process, IoCommandKind.dbTransaction, iocUid, null, ex);
                                throw;
                            }
                        }
                        else
                        {
                            var transactionId = Transaction.Current?.ToIdentifierString();
                            var iocUid = Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "rollback", transactionId, null,
                                "removing transaction suppression");

                            if (LogSeverity != LogSeverity.Verbose)
                                Context.Log(LogSeverity, Process, "removing transaction suppression");

                            try
                            {
                                _scope.Dispose();
                                _scope = null;
                                Context.RegisterIoCommandSuccess(Process, IoCommandKind.dbTransaction, iocUid, null);
                                Context.Log(LogSeverity, Process, "suppression removed");
                            }
                            catch (Exception ex)
                            {
                                _scope = null;
                                Context.RegisterIoCommandFailed(Process, IoCommandKind.dbTransaction, iocUid, null, ex);
                                throw;
                            }
                        }
                    }
                    else
                    {
                        if (LogSeverity != LogSeverity.Verbose)
                            Context.Log(LogSeverity, Process, "completing transaction");

                        var iocUid = Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, Convert.ToInt32(Timeout.TotalSeconds), "commit", completitionTransactionId, null, "committing transaction");
                        try
                        {
                            _scope.Dispose();
                            _scope = null;
                            Context.RegisterIoCommandSuccess(Process, IoCommandKind.dbTransaction, iocUid, null);
                            Context.Log(LogSeverity, Process, "transaction committed");
                        }
                        catch (Exception ex)
                        {
                            _scope = null;
                            Context.RegisterIoCommandFailed(Process, IoCommandKind.dbTransaction, iocUid, null, ex);
                            throw;
                        }
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
