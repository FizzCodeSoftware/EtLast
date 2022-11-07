namespace FizzCode.EtLast;

public abstract class AbstractEtlTask : AbstractProcess, IEtlTask
{
    private readonly ExecutionStatistics _statistics = new();
    public IExecutionStatistics Statistics => _statistics;

    public Dictionary<IoCommandKind, IoCommandCounter> IoCommandCounters => _ioCommandCounterCollection.Counters;

    private readonly IoCommandCounterCollection _ioCommandCounterCollection = new();

    public abstract IEnumerable<IProcess> CreateJobs();

    protected AbstractEtlTask()
    {
    }

    public override void Execute(IProcess caller, Pipe pipe)
    {
        Context.RegisterProcessInvocationStart(this, caller);
        Pipe = pipe ?? caller?.Pipe ?? new Pipe(Context);

        LogCall(caller);
        LogPublicSettableProperties(LogSeverity.Debug);

        var netTimeStopwatch = Stopwatch.StartNew();
        try
        {
            _statistics.Start();

            ValidateParameters();

            Context.Listeners.Add(_ioCommandCounterCollection);
            try
            {
                var jobs = CreateJobs()?
                    .Where(x => x != null)
                    .ToList();

                if (jobs?.Count > 0)
                {
                    for (var jobIndex = 0; jobIndex < jobs.Count; jobIndex++)
                    {
                        var job = jobs[jobIndex];

                        job.Execute(this);

                        if (Pipe.IsTerminating)
                            break;
                    }
                }
            }
            finally
            {
                Context.Listeners.Remove(_ioCommandCounterCollection);
            }

            _statistics.Finish();

            Context.Log(LogSeverity.Information, this, "{TaskResult} in {Elapsed}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                Pipe.ToLogString(), Statistics.RunTime, Statistics.CpuTime, Statistics.TotalAllocations, Statistics.AllocationDifference);

            LogPrivateSettableProperties(LogSeverity.Debug);
        }
        finally
        {
            netTimeStopwatch.Stop();
            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
        }
    }
}