namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class EtlPluginResult
    {
        public bool Success { get; set; } = true;
        public bool TerminatePluginScope { get; set; } = false;
        public bool TerminateGlobalScope { get; set; } = false;
        public List<Exception> Exceptions { get; set; } = new List<Exception>();

        public void MergeWith(EtlPluginResult otherResult)
        {
            Success &= otherResult.Success;
            TerminatePluginScope |= otherResult.TerminatePluginScope;
            TerminateGlobalScope |= otherResult.TerminateGlobalScope;
            Exceptions.AddRange(otherResult.Exceptions);
        }
    }
}