namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class EtlContextResult
    {
        public bool Success { get; set; } = true;
        public bool TerminateHost { get; set; } = false;
        public List<Exception> Exceptions { get; set; } = new List<Exception>();

        public void MergeWith(EtlContextResult otherResult)
        {
            Success &= otherResult.Success;
            TerminateHost |= otherResult.TerminateHost;
            Exceptions.AddRange(otherResult.Exceptions);
        }
    }
}