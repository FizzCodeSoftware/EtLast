namespace FizzCode.EtLast;

public class TaskResult<T> : ProcessResult
    where T : IEtlTask
{
    public T Task { get; init; }
}
