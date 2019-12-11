namespace FizzCode.EtLast
{
    using System;

    public class BasicScopeProcessFailedEventArgs : EventArgs
    {
        public IBasicScope Scope { get; }
        public IExecutable Process { get; }

        public BasicScopeProcessFailedEventArgs(IBasicScope scope, IExecutable process)
        {
            Scope = scope;
            Process = process;
        }
    }
}