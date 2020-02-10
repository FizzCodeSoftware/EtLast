namespace FizzCode.EtLast
{
    using System;

    public class BasicScopeProcessFailedEventArgs : EventArgs
    {
        public BasicScope Scope { get; }
        public IExecutable Process { get; }

        public BasicScopeProcessFailedEventArgs(BasicScope scope, IExecutable process)
        {
            Scope = scope;
            Process = process;
        }
    }
}