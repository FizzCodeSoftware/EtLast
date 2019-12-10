namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class EtlContextResult
    {
        public bool Success { get; internal set; } = true;
        public bool TerminateHost { get; set; }
        public List<Exception> Exceptions { get; } = new List<Exception>();
        public int WarningCount { get; internal set; }
    }
}