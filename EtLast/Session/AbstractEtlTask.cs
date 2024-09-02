namespace FizzCode.EtLast;

public abstract class AbstractEtlTask : AbstractProcess, IEtlTask
{
    private readonly ExecutionStatistics _statistics = new();
    public IExecutionStatistics Statistics => _statistics;

    public IReadOnlyDictionary<IoCommandKind, IoCommandCounter> IoCommandCounters => _ioCommandCounterCollection.Counters;

    private readonly IoCommandKindCounter _ioCommandCounterCollection = new();

    public abstract void Execute(IFlow flow);

    protected AbstractEtlTask()
    {
        PublicSettablePropertyLogSeverity = LogSeverity.Debug;
    }

    public override void Execute(ICaller caller, FlowState flowState = null)
    {
        BeginExecution(caller, flowState);
        if (FlowState.IsTerminating)
            return;

        var netTimeStopwatch = Stopwatch.StartNew();
        _statistics.Start();

        try
        {
            ValidateParameterAnnotations();
            ValidateParameters();
        }
        catch (Exception ex)
        {
            FlowState.AddException(this, ex);
        }

        if (!FlowState.IsTerminating)
        {
            Context.Listeners.Add(_ioCommandCounterCollection);
            try
            {
                var flow = new Flow(Context, this, FlowState);
                Execute(flow);
            }
            catch (Exception ex)
            {
                FlowState.AddException(this, ex);
            }
            finally
            {
                Context.Listeners.Remove(_ioCommandCounterCollection);
            }

            _statistics.Finish();

            LogResult(netTimeStopwatch);

            Context.Log(LogSeverity.Debug, this, "elapsed: {Elapsed}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                Statistics.RunTime, Statistics.CpuTime, Statistics.TotalAllocations, Statistics.AllocationDifference);

            LogPrivateSettableProperties(LogSeverity.Debug);
        }
        else
        {
            netTimeStopwatch.Stop();
            Context.RegisterProcessEnd(this, netTimeStopwatch.ElapsedMilliseconds);
        }
    }
}