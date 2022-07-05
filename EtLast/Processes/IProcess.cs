namespace FizzCode.EtLast;

public interface IProcess
{
    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public ProcessInvocationInfo InvocationInfo { get; set; }

    public IEtlContext Context { get; }
    public List<Exception> Exceptions { get; }

    public string Name { get; }
    public string Kind { get; }
    public string GetTopic();
}
