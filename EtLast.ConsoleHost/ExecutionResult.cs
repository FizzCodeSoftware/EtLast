namespace FizzCode.EtLast.ConsoleHost;

public class ExecutionResult : IExecutionResult
{
    public ExecutionStatusCode Status { get; set; } = ExecutionStatusCode.Success;
    public List<TaskExectionResult> TaskResults { get; } = [];

    public ExecutionResult()
    {
    }

    public ExecutionResult(ExecutionStatusCode status)
    {
        Status = status;
    }
}