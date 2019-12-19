namespace FizzCode.EtLast
{
    using System;

    public class ContextExceptionEventArgs : EventArgs
    {
        public IProcess Process { get; set; }
        public IBaseOperation Operation { get; set; }
        public Exception Exception { get; set; }
    }
}