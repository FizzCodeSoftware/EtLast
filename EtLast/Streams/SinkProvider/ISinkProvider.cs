﻿namespace FizzCode.EtLast
{
    public interface ISinkProvider
    {
        public string Topic { get; }
        public NamedSink GetSink(IProcess caller, string partitionKey);
        public bool AutomaticallyDispose { get; }
    }
}