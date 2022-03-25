namespace FizzCode.EtLast;

using System;
using System.Collections.Generic;
using System.Transactions;

public sealed class EtlTransactionScope : IDisposable
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

        if (Kind == TransactionScopeKind.Suppress && previousId == null)
        {
            return;
        }

        Scope = new TransactionScope((TransactionScopeOption)Kind, scopeTimeout);

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

            Context.RegisterIoCommandSuccess(Process, IoCommandKind.dbTransaction, iocUid, null);
        }
        catch (Exception ex)
        {
            Completed = false;
            Context.RegisterIoCommandFailed(Process, IoCommandKind.dbTransaction, iocUid, null, ex);
        }
    }

    public void Dispose(bool disposing)
    {
        if (!_isDisposed)
        {
            if (disposing)
            {
                if (Scope != null)
                {
                    var iocUid = 0;
                    if (Kind != TransactionScopeKind.Suppress && !CompleteCalled)
                    {
                        var transactionId = Transaction.Current?.ToIdentifierString();
                        iocUid = Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "reverting transaction", transactionId, null,
                            "reverting transaction");
                    }

                    if (Kind == TransactionScopeKind.Suppress && !CompleteCalled)
                    {
                        var transactionId = Transaction.Current?.ToIdentifierString();
                        iocUid = Context.RegisterIoCommandStart(Process, IoCommandKind.dbTransaction, null, null, "removing transaction suppression", transactionId, null,
                            "removing transaction suppression");
                    }

                    try
                    {
                        Scope.Dispose();
                        Scope = null;

                        if (iocUid != 0)
                        {
                            Context.RegisterIoCommandSuccess(Process, IoCommandKind.dbTransaction, iocUid, null);
                        }
                    }
                    catch (Exception ex)
                    {
                        if (iocUid != 0)
                        {
                            Context.RegisterIoCommandFailed(Process, IoCommandKind.dbTransaction, iocUid, null, ex);
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
