namespace FizzCode.EtLast;

public class IsolatedFlowContext
{
    public required FlowState ParentFlowState { get; init; }
    public required IFlowStarter IsolatedFlow { get; init; }
}