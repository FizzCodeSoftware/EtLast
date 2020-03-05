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

        public ProcessExecutionException(IProcess process, IReadOnlySlimRow row, Exception innerException)
            : this(process, "error raised during the execution of a process", innerException)
        {
            Data.Add("Row", row.ToDebugString());
        }

        public ProcessExecutionException(IProcess process, string message)
            : base(process, message)
        {
        }

        public ProcessExecutionException(IProcess process, IReadOnlySlimRow row, string message)
            : base(process, message)
        {
            Data.Add("Row", row.ToDebugString());
        }

        public ProcessExecutionException(IProcess process, string message, Exception innerException)
            : base(process, message, innerException)
        {
        }

        public ProcessExecutionException(IProcess process, IReadOnlySlimRow row, string message, Exception innerException)
            : base(process, message, innerException)
        {
            Data.Add("Row", row.ToDebugString());
        }

        public static ProcessExecutionException Wrap(IProcess process, IReadOnlySlimRow row, Exception ex)
        {
            if (ex is ProcessExecutionException pex && (pex.Data["Row"] is string rowString) && string.Equals(rowString, row.ToDebugString(), StringComparison.Ordinal))
                return pex;

            return new ProcessExecutionException(process, row, ex);
        }

        public static EtlException Wrap(IProcess process, Exception ex)
        {
            if (ex is InvalidProcessParameterException ppe)
                return ppe;

            return (ex is ProcessExecutionException pex)
                ? pex
                : new ProcessExecutionException(process, ex);
        }
    }
}