namespace FizzCode.EtLast;

public interface IProcess
{
    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public ProcessInvocationInfo InvocationInfo { get; set; }

    public ProcessInvocationContext InvocationContext { get; }
    public bool Success => InvocationContext?.IsTerminating != true;

    public IEtlContext Context { get; }

    public string Name { get; }
    public string Kind { get; }
    public string GetTopic();
}