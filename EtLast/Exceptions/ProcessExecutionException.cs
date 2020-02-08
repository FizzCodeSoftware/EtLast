namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class ProcessExecutionException : EtlException
    {
        public ProcessExecutionException(IProcess process, Exception innerException)
            : this(process, "error raised during the execution of a process", innerException)
        {
        }

        public ProcessExecutionException(IProcess process, IRow row, Exception innerException)
            : this(process, "error raised during the execution of a process", innerException)
        {
            Data.Add("Row", row.ToDebugString());
        }

        public ProcessExecutionException(IProcess process, string message)
            : base(process, message)
        {
        }

        public ProcessExecutionException(IProcess process, IRow row, string message)
            : base(process, message)
        {
            Data.Add("Row", row.ToDebugString());
        }

        public ProcessExecutionException(IProcess process, string message, Exception innerException)
            : base(process, message, innerException)
        {
        }

        public ProcessExecutionException(IProcess process, IRow row, string message, Exception innerException)
            : base(process, message, innerException)
        {
            Data.Add("Row", row.ToDebugString());
        }
    }
}