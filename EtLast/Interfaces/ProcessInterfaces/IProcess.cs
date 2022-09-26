namespace FizzCode.EtLast;

public interface IProcess
{
    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public ProcessInvocationInfo InvocationInfo { get; set; }

    public Pipe Pipe { get; }
    public bool Success => Pipe?.IsTerminating != true;

    public IEtlContext Context { get; }

    public string Name { get; }
    public string Kind { get; }
    public string GetTopic();

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public void SetContext(IEtlContext context, bool onlyNull = true);

    public void Execute(IProcess caller);
    public void Execute(IProcess caller, Pipe pipe);
}