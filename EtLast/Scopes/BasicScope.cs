namespace FizzCode.EtLast;

using System;
using System.Collections.Generic;
using System.Linq;

public delegate IEnumerable<IExecutable> ProcessCreatorDelegate(BasicScope scope);

/// <summary>
/// The default etl scope to execute multiple processes, optionally supporting ambient transaction scopes.
/// </summary>
public sealed class BasicScope : AbstractExecutable, IScope
{
    public ProcessCreatorDelegate ProcessCreator { get; set; }
    public IEnumerable<ProcessCreatorDelegate> ProcessCreators { get; set; }

    /// <summary>
    /// Default value is <see cref="TransactionScopeKind.None"/>.
    /// </summary>
    public TransactionScopeKind TransactionScopeKind { get; set; } = TransactionScopeKind.None;

    /// <summary>
    /// Default value is <see cref="TransactionScopeKind.None"/> which means creation of the executables happens directly within the exection scope.
    /// </summary>
    public TransactionScopeKind CreationTransactionScopeKind { get; set; } = TransactionScopeKind.None;

    /// <summary>
    /// Default value is true.
    /// </summary>
    public bool StopOnError { get; set; } = true;

    public EventHandler<BasicScopeProcessFailedEventArgs> OnError { get; set; }

    public BasicScope(IEtlContext context, string topic, string name = null)
        : base(context)
    {
    }

    protected override void ValidateImpl()
    {
    }

    protected override void ExecuteImpl()
    {
        using (var scope = Context.BeginScope(this, TransactionScopeKind, LogSeverity.Information))
        {
            var failed = false;

            var creators = new List<ProcessCreatorDelegate>();
            if (ProcessCreator != null)
                creators.Add(ProcessCreator);

            if (ProcessCreators != null)
                creators.AddRange(ProcessCreators);

            foreach (var creator in creators)
            {
                IExecutable[] processes = null;
                using (var creatorScope = Context.BeginScope(this, CreationTransactionScopeKind, LogSeverity.Information))
                {
                    processes = creator.Invoke(this).Where(x => x != null).ToArray();
                }

                if (processes.Length == 0)
                    continue;

                foreach (var process in processes)
                {
                    var initialExceptionCount = Context.ExceptionCount;

                    process.Execute(this);

                    if (Context.ExceptionCount != initialExceptionCount)
                    {
                        OnError?.Invoke(this, new BasicScopeProcessFailedEventArgs(this, process));

                        failed = true;
                        if (StopOnError)
                            break;
                    }
                }
            }

            if (!failed)
            {
                scope.Complete();
            }
        }
    }
}
