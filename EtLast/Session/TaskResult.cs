namespace FizzCode.EtLast;

public class TaskWithResult<T> : ProcessResult
    where T : IEtlTask
{
    public T Task { get; init; }

    public TaskWithResult(ProcessResult result, T task)
    {
        Task = task;
        Exceptions.AddRange(result.Exceptions);
    }
}