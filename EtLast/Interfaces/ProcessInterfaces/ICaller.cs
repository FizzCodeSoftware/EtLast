namespace FizzCode.EtLast;

public interface ICaller
{
    FlowState GetFlowState();
    IEtlContext Context { get; }
}