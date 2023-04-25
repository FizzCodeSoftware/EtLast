namespace FizzCode.EtLast;

public class ScopeAction
{
    public required IEtlContext Context { get; init; }
    public required IProcess Caller { get; init; }
    public required IScope Scope { get; init; }
    public required string Topic { get; init; }
    public required string Action { get; init; }
    public required IProcess Process { get; init; }
}