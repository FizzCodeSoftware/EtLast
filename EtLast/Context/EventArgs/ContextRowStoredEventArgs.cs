namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class ContextRowStoredEventArgs : EventArgs
    {
        public IRow Row { get; set; }
        public List<KeyValuePair<string, string>> Location { get; set; }
    }
}