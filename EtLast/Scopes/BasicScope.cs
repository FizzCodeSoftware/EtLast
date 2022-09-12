namespace FizzCode.EtLast;

public delegate IEnumerable<IJob> ProcessCreatorDelegate(BasicScope scope);

/// <summary>
/// The default etl scope to execute multiple jobs, optionally supporting ambient transaction scopes.
/// </summary>
public sealed class BasicScope : AbstractJob, IScope
{
    public ProcessCreatorDelegate JobCreator { get; set; }
    public IEnumerable<ProcessCreatorDelegate> JobCreators { get; set; }

    /// <summary>
    /// Default value is <see cref="TransactionScopeKind.None"/>.
    /// </summary>
    public TransactionScopeKind TransactionScopeKind { get; set; } = TransactionScopeKind.None;

    /// <summary>
    /// Default value is <see cref="TransactionScopeKind.None"/> which means creation of the jobs happens directly within the caller scope.
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

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        try
        {
            using (var scope = Context.BeginScope(this, TransactionScopeKind, LogSeverity.Information))
            {
                var creators = new List<ProcessCreatorDelegate>();
                if (JobCreator != null)
                    creators.Add(JobCreator);

                if (JobCreators != null)
                    creators.AddRange(JobCreators);

                var success = true;
                foreach (var creator in creators)
                {
                    IJob[] jobs = null;
                    using (var creatorScope = Context.BeginScope(this, CreationTransactionScopeKind, LogSeverity.Information))
                    {
                        jobs = creator.Invoke(this).Where(x => x != null).ToArray();
                    }

                    if (jobs.Length == 0)
                        continue;

                    foreach (var job in jobs)
                    {
                        var jobInvocationContext = new ProcessInvocationContext(Context);
                        job.Execute(this, jobInvocationContext);

                        if (InvocationContext.Failed)
                        {
                            OnError?.Invoke(this, new BasicScopeProcessFailedEventArgs(this, job));
                        }

                        if (StopOnError)
                            InvocationContext.TakeExceptions(jobInvocationContext);

                        if (InvocationContext.IsTerminating)
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
            InvocationContext.AddException(this, ex);
        }
    }
}