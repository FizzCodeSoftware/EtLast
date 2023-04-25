namespace FizzCode.EtLast;

public abstract class AbstractEtlTask : AbstractProcess, IEtlTask
{
    private readonly ExecutionStatistics _statistics = new();
    public IExecutionStatistics Statistics => _statistics;

    public IReadOnlyDictionary<IoCommandKind, IoCommandCounter> IoCommandCounters => _ioCommandCounterCollection.Counters;

    private readonly IoCommandCounterCollection _ioCommandCounterCollection = new();

    public abstract void Execute(IFlow flow);

    protected AbstractEtlTask()
    {
    }

    public override void Execute(IProcess caller, FlowState flowState)
    {
        Context.RegisterProcessInvocationStart(this, caller);
        FlowState = flowState ?? caller?.FlowState ?? new FlowState(Context);

        LogCall(caller);
        LogPublicSettableProperties(LogSeverity.Debug);

        var netTimeStopwatch = Stopwatch.StartNew();
        _statistics.Start();

        try
        {
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
                var pe = new Flow(Context, this, FlowState);
                Execute(pe);
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
            Context.Log(LogSeverity.Information, this, "{TaskResult} in {Elapsed}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                FlowState.StatusToLogString(), Statistics.RunTime, Statistics.CpuTime, Statistics.TotalAllocations, Statistics.AllocationDifference);

            LogPrivateSettableProperties(LogSeverity.Debug);
        }

        netTimeStopwatch.Stop();
        Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
    }
}