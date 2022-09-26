namespace FizzCode.EtLast;

public sealed class BasicScopeProcessFailedEventArgs : EventArgs
{
    public BasicScope Scope { get; }
    public IProcess Process { get; }

    public BasicScopeProcessFailedEventArgs(BasicScope scope, IProcess process)
    {
        Scope = scope;
        Process = process;
    }
}