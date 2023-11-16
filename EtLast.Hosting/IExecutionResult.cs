namespace FizzCode.EtLast.ConsoleHost;

public interface IExecutionResult
{
    public ExecutionStatusCode Status { get; }
    public List<TaskExectionResult> TaskResults { get; }
}

public class TaskExectionResult(IEtlTask task)
{
    public Type TaskType { get; } = task.GetType();
    public string TaskName { get; } = task.Name;
    public string TaskKind { get; } = task.Kind;
    public string TaskTopic { get; } = task.GetTopic();
    public IExecutionStatistics Statistics { get; } = task.Statistics;
    public IReadOnlyDictionary<IoCommandKind, IoCommandCounter> IoCommandCounters { get; } = task.IoCommandCounters;
    public List<Exception> Exceptions { get; } = task.FlowState.Exceptions.ToList();
}