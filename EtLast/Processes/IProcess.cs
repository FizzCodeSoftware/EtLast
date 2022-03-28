namespace FizzCode.EtLast;

public interface IProcess
{
    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    ProcessInvocationInfo InvocationInfo { get; set; }

    IEtlContext Context { get; }
    string Name { get; }

    string Kind { get; }

    string GetTopic();
}
