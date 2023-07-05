namespace FizzCode.EtLast;

public interface IProcess
{
    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public ProcessInvocationInfo InvocationInfo { get; set; }

    public FlowState FlowState { get; }

    public IEtlContext Context { get; }

    public string Name { get; }
    public string InvocationName => InvocationInfo.InvocationUid + "~" + Name;

    public string Kind { get; }
    public string GetTopic();

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public void SetContext(IEtlContext context, bool onlyNull = true);

    public void Execute(IProcess caller);
    public void Execute(IProcess caller, FlowState flowState);
}