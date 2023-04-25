namespace FizzCode.EtLast;

public delegate IEnumerable<IProcess> ProcessCreatorDelegate(BasicScope scope);

/// <summary>
/// The default etl scope to execute multiple jobs, optionally supporting ambient transaction scopes.
/// </summary>
public sealed class BasicScope : AbstractJob, IScope
{
    public ProcessCreatorDelegate JobCreator { get; set; }
    public IEnumerable<ProcessCreatorDelegate> JobCreators { get; set; }

    /// <summary>
    /// Default value is <see cref="TransactionScopeKind.Required"/>
    /// </summary>
    public required TransactionScopeKind TransactionScopeKind { get; init; } = TransactionScopeKind.Required;

    /// <summary>
    /// Default value is <see cref="TransactionScopeKind.None"/>
    /// </summary>
    public required TransactionScopeKind CreationTransactionScopeKind { get; init; } = TransactionScopeKind.None;

    /// <summary>
    /// Default value is true.
    /// </summary>
    public required bool StopOnError { get; init; } = true;

    public EventHandler<BasicScopeProcessFailedEventArgs> OnFailure { get; set; }

    public BasicScope(IEtlContext context)
        : base(context)
    {
    }

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        try
        {
            using (var scope = Context.BeginTransactionScope(this, TransactionScopeKind, LogSeverity.Information))
            {
                var creators = new List<ProcessCreatorDelegate>();
                if (JobCreator != null)
                    creators.Add(JobCreator);

                if (JobCreators != null)
                    creators.AddRange(JobCreators);

                var success = true;
                foreach (var creator in creators)
                {
                    IProcess[] processList = null;
                    using (var creatorScope = Context.BeginTransactionScope(this, CreationTransactionScopeKind, LogSeverity.Information))
                    {
                        processList = creator.Invoke(this).Where(x => x != null).ToArray();
                    }

                    if (processList.Length == 0)
                        continue;

                    foreach (var process in processList)
                    {
                        var isolatedFlow = new FlowState(Context);
                        process.Execute(this, isolatedFlow);

                        if (isolatedFlow.Failed)
                        {
                            OnFailure?.Invoke(this, new BasicScopeProcessFailedEventArgs(this, process));
                        }

                        if (StopOnError)
                            FlowState.TakeExceptions(isolatedFlow);

                        if (FlowState.IsTerminating)
                        {
                            success = false;
                            break;
                        }
                    }
                }

                if (success)
                    scope.Complete();
            } // dispose scope
        }
        catch (Exception ex)
        {
            FlowState.AddException(this, ex);
        }
    }
}