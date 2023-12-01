namespace FizzCode.EtLast;

public interface ICaller
{
    FlowState FlowState { get; }
    IEtlContext Context { get; }
}