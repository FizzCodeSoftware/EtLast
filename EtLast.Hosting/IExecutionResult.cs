namespace FizzCode.EtLast;

public interface IExecutionResult
{
    public ExecutionStatusCode Status { get; }
    public List<TaskExecutionResult> TaskResults { get; }
    public ContextManifest ContextManifest { get; }
}

public class TaskExecutionResult(IEtlTask task)
{
    public Type TaskType { get; } = task.GetType();
    public string TaskName { get; } = task.Name;
    public string TaskKind { get; } = task.Kind;
    public string TaskTopic { get; } = task.GetTopic();
    public IExecutionStatistics Statistics { get; } = task.Statistics;
    public IReadOnlyDictionary<IoCommandKind, IoCommandCounter> IoCommandCounters { get; } = task.IoCommandCounters;
    public bool Failed { get; } = task.FlowState.Failed;
    public List<Exception> Exceptions { get; } = [.. task.FlowState.Exceptions];
}