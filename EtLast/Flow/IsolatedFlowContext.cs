namespace FizzCode.EtLast;

public class IsolatedFlowContext
{
    public required FlowState ParentFlowState { get; init; }
    public required IFlow IsolatedFlow { get; init; }
}