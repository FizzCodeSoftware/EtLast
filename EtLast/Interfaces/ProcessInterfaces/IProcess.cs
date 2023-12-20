namespace FizzCode.EtLast;

public interface IProcess : ICaller
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public ProcessInvocationInfo InvocationInfo { get; set; }

    public string Name { get; }
    public string InvocationName => InvocationInfo.InvocationId + "~" + Name;

    public string Kind { get; }
    public string GetTopic();

    public void Execute(ICaller caller, FlowState flowState = null);
}