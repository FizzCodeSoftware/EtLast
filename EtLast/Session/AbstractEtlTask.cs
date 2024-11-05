namespace FizzCode.EtLast;

public abstract class AbstractEtlTask : AbstractProcess, IEtlTask
{
    private readonly ExecutionStatistics _statistics = new();
    public IExecutionStatistics Statistics => _statistics;

    public IEnumerable<IIoCommandCounter> IoCommandCounters => _ioCommandListener.Counters;

    private readonly IoCommandListener _ioCommandListener = new();

    public Action<IFlow> ExecuteBefore { get; init; }
    public Action<IFlow> ExecuteAfter { get; init; }

    public abstract void Execute(IFlow flow);

    protected AbstractEtlTask()
    {
        PublicSettablePropertyLogSeverity = LogSeverity.Debug;
        CallLogSeverity = LogSeverity.Information;
    }

    public override void Execute(ICaller caller, FlowState flowState = null)
    {
        BeginExecution(caller, flowState);
        if (FlowState.IsTerminating)
        {
            Context.RegisterProcessEnd(this, 0);
            return;
        }

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
            Context.Listeners.Add(_ioCommandListener);
            try
            {
                var flow = new Flow(Context, this, FlowState);
                ExecuteBefore?.Invoke(flow);

                Execute(flow);

                ExecuteAfter?.Invoke(flow);
            }
            catch (Exception ex)
            {
                FlowState.AddException(this, ex);
            }
            finally
            {
                Context.Listeners.Remove(_ioCommandListener);
            }

            _statistics.Finish();

            LogResult(netTimeStopwatch);

            Context.Log(LogSeverity.Debug, this, "elapsed: {Elapsed}, CPU time: {CpuTime}, total allocations: {AllocatedMemory}, allocation difference: {MemoryDifference}",
                Statistics.RunTime, Statistics.CpuTime, Statistics.TotalAllocations, Statistics.AllocationDifference);

            LogPrivateSettableProperties(LogSeverity.Debug);
        }
        else
        {
            _statistics.Finish();
            netTimeStopwatch.Stop();
            Context.RegisterProcessEnd(this, netTimeStopwatch.ElapsedMilliseconds);
        }
    }
}