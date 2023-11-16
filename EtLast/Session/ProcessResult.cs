namespace FizzCode.EtLast;

public class ProcessResult
{
    public List<Exception> Exceptions { get; } = [];
    public bool Success => Exceptions.Count == 0;
}