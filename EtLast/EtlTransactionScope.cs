namespace FizzCode.EtLast;

public sealed class EtlTransactionScope : IDisposable
{
    public IEtlContext Context { get; }
    public IProcess Process { get; }
    public TransactionScopeKind Kind { get; }
    public LogSeverity LogSeverity { get; }
    public TimeSpan Timeout { get; }

    public bool CompleteCalled { get; private set; }
    private int _completionIocUid;

    private TransactionScope _scope;
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

        if (Kind == TransactionScopeKind.Suppress && previousId == null)
        {
            return;
        }

        _scope = new TransactionScope((TransactionScopeOption)Kind, scopeTimeout);

        var newId = Transaction.Current?.ToIdentifierString();

        var iocUid = 0;
        switch (kind)
        {
            case TransactionScopeKind.RequiresNew:
                iocUid = Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "new transaction started", newId, null,
                    "new transaction started");
                break;
            case TransactionScopeKind.Required:
                iocUid = previousId == null || newId != previousId
                    ? Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "new transaction started", newId, null,
                        "new transaction started")
                    : Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "new transaction started and merged with previous", newId, () => new[] { new KeyValuePair<string, object>("previous transaction", previousId) },
                        "new transaction started and merged with previous");
                break;
            case TransactionScopeKind.Suppress:
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

        var transactionId = Transaction.Current.ToIdentifierString();

        CompleteCalled = true;

        _completionIocUid = Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, Convert.ToInt32(Timeout.TotalSeconds), "commit", transactionId, null, "completing transaction");
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
                    var iocUid = _completionIocUid;

                    if (Kind != TransactionScopeKind.Suppress && !CompleteCalled)
                    {
                        var transactionId = Transaction.Current?.ToIdentifierString();
                        iocUid = Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "rollback", transactionId, null,
                            "reverting transaction");
                    }

                    if (Kind == TransactionScopeKind.Suppress && !CompleteCalled)
                    {
                        var transactionId = Transaction.Current?.ToIdentifierString();
                        iocUid = Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "rollback", transactionId, null,
                            "removing transaction suppression");
                    }

                    try
                    {
                        _scope.Dispose();
                        _scope = null;

                        if (iocUid != 0)
                            Context.RegisterIoCommandSuccess(Process, IoCommandKind.dbTransaction, iocUid, null);
                    }
                    catch (Exception ex)
                    {
                        _scope = null;

                        if (iocUid != 0)
                            Context.RegisterIoCommandFailed(Process, IoCommandKind.dbTransaction, iocUid, null, ex);

                        throw;
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
