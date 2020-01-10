namespace FizzCode.EtLast
{
    using System;

    public class ContextRowOwnerChangedEventArgs : EventArgs
    {
        public IRow Row { get; set; }
        public IProcess PreviousProcess { get; set; }
        public IProcess CurrentProcess { get; set; }
    }
}