namespace FizzCode.EtLast
{
    using System;

    public class ContextRowCreatedEventArgs : EventArgs
    {
        public IRow Row { get; set; }
        public IProcess CreatorProcess { get; set; }
    }
}