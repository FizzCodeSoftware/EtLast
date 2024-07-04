namespace FizzCode.EtLast;

public interface IProcess : ICaller
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public ProcessExecutionInfo ExecutionInfo { get; set; }

    public string Name { get; }
    public string UniqueName => ExecutionInfo.Id + "~" + Name;

    public string Kind { get; }
    public string GetTopic();

    public void Execute(ICaller caller, FlowState flowState = null);
}