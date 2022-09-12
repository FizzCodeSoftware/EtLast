namespace FizzCode.EtLast;

public class ProcessResult
{
    public List<Exception> Exceptions { get; } = new List<Exception>();
    public bool Success => Exceptions.Count == 0;
}