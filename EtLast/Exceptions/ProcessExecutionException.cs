namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class ProcessExecutionException : EtlException
    {
        public ProcessExecutionException(IProcess process, Exception innerException)
            : this(process, "error raised during the execution of a block", innerException)
        {
        }

        public ProcessExecutionException(IProcess process, string message)
            : base(process, message)
        {
        }

        public ProcessExecutionException(IProcess process, string message, Exception innerException)
            : base(process, message, innerException)
        {
        }
    }
}