namespace FizzCode.EtLast;

public class ScopeAction
{
    public IEtlContext Context { get; init; }
    public IProcess Caller { get; init; }
    public IScope Scope { get; init; }
    public string Topic { get; init; }
    public string Action { get; init; }
}