namespace FizzCode.EtLast.ConsoleHost;

public interface IExecutionResult
{
    public ExecutionStatusCode Status { get; }
    public List<TaskExectionResult> TaskResults { get; }
}

public class TaskExectionResult
{
    public Type TaskType { get; }
    public string TaskName { get; }
    public string TaskKind { get; }
    public string TaskTopic { get; }
    public IExecutionStatistics Statistics { get; }
    public Dictionary<IoCommandKind, IoCommandCounter> IoCommandCounters { get; }
    public List<Exception> Exceptions { get; }

    public TaskExectionResult(IEtlTask task, ProcessResult result)
    {
        TaskType = task.GetType();
        TaskName = task.Name;
        TaskKind = task.Kind;
        TaskTopic = task.GetTopic();
        Statistics = task.Statistics;
        IoCommandCounters = task.IoCommandCounters;
        Exceptions = result.Exceptions.ToList();
    }
}