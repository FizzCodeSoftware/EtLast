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

    public EtlTransactionScope(IProcess process, TransactionScopeKind kind, LogSeverity logSeverity, TimeSpan? timeoutOverride = null)
    {
        Context = process.Context;
        Process = process;
        Kind = kind;
        LogSeverity = logSeverity;
        Timeout = timeoutOverride ?? Context.TransactionScopeTimeout;

        if (Kind == TransactionScopeKind.None)
            return;

        var previousId = Transaction.Current?.ToIdentifierString();

        if (Kind == TransactionScopeKind.Suppress && previousId == null)
            return;

        _scope = new TransactionScope((TransactionScopeOption)Kind, Timeout);

        var newId = Transaction.Current?.ToIdentifierString();

        IoCommand ioCommand = null;
        switch (kind)
        {
            case TransactionScopeKind.RequiresNew:
                if (logSeverity != LogSeverity.Verbose)
                    Context.Log(logSeverity, Process, "new transaction started");

                ioCommand = Context.RegisterIoCommand(new IoCommand()
                {
                    Process = Process,
                    Kind = IoCommandKind.dbTransaction,
                    TransactionId = newId,
                    Message = "new transaction started",
                });

                break;
            case TransactionScopeKind.Required:
                if (logSeverity != LogSeverity.Verbose)
                    Context.Log(logSeverity, Process, "new transaction started" + (previousId != null && newId == previousId ? " and merged with previous" : ""));

                ioCommand = previousId == null || newId != previousId
                    ? Context.RegisterIoCommand(new IoCommand()
                    {
                        Process = Process,
                        Kind = IoCommandKind.dbTransaction,
                        TransactionId = newId,
                        Message = "new transaction started"
                    })
                    : Context.RegisterIoCommand(new IoCommand()
                    {
                        Process = Process,
                        Kind = IoCommandKind.dbTransaction,
                        TransactionId = newId,
                        ArgumentListGetter = () => new[] { new KeyValuePair<string, object>("previous transaction", previousId) },
                        Message = "new transaction started and merged with previous"
                    });

                break;
            case TransactionScopeKind.Suppress:
                if (logSeverity != LogSeverity.Verbose)
                    Context.Log(logSeverity, Process, "existing transaction suppressed");

                ioCommand = Context.RegisterIoCommand(new IoCommand()
                {
                    Process = Process,
                    Kind = IoCommandKind.dbTransaction,
                    TransactionId = previousId,
                    Message = "existing transaction suppressed"
                });

                break;
        }

        ioCommand?.End();
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
                            var ioCommand = Context.RegisterIoCommand(new IoCommand()
                            {
                                Process = Process,
                                Kind = IoCommandKind.dbTransaction,
                                TransactionId = transactionId,
                                Message = "reverting transaction",
                            });

                            if (LogSeverity != LogSeverity.Verbose)
                                Context.Log(LogSeverity, Process, "reverting transaction");

                            try
                            {
                                _scope.Dispose();
                                _scope = null;

                                ioCommand.End();
                                Context.Log(LogSeverity, Process, "transaction reverted");
                            }
                            catch (Exception ex)
                            {
                                _scope = null;
                                ioCommand.Failed(ex);
                                throw;
                            }
                        }
                        else
                        {
                            var transactionId = Transaction.Current?.ToIdentifierString();
                            var ioCommand = Context.RegisterIoCommand(new IoCommand()
                            {
                                Process = Process,
                                Kind = IoCommandKind.dbTransaction,
                                TransactionId = transactionId,
                                Message = "removing transaction suppression",
                            });

                            if (LogSeverity != LogSeverity.Verbose)
                                Context.Log(LogSeverity, Process, "removing transaction suppression");

                            try
                            {
                                _scope.Dispose();
                                _scope = null;
                                ioCommand.End();
                                Context.Log(LogSeverity, Process, "suppression removed");
                            }
                            catch (Exception ex)
                            {
                                _scope = null;
                                ioCommand.Failed(ex);
                                throw;
                            }
                        }
                    }
                    else
                    {
                        if (LogSeverity != LogSeverity.Verbose)
                            Context.Log(LogSeverity, Process, "completing transaction");

                        var ioCommand = Context.RegisterIoCommand(new IoCommand()
                        {
                            Process = Process,
                            Kind = IoCommandKind.dbTransaction,
                            TimeoutSeconds = Convert.ToInt32(Timeout.TotalSeconds),
                            TransactionId = completitionTransactionId,
                            Message = "committing transaction",
                        });

                        try
                        {
                            _scope.Dispose();
                            _scope = null;
                            ioCommand.End();
                            Context.Log(LogSeverity, Process, "transaction committed");
                        }
                        catch (Exception ex)
                        {
                            _scope = null;
                            ioCommand.Failed(ex);
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
