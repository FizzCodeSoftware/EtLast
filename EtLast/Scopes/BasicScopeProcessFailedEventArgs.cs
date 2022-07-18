namespace FizzCode.EtLast;

public sealed class BasicScopeProcessFailedEventArgs : EventArgs
{
    public BasicScope Scope { get; }
    public IJob Process { get; }

    public BasicScopeProcessFailedEventArgs(BasicScope scope, IJob process)
    {
        Scope = scope;
        Process = process;
    }
}