namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class OperationExecutionException : EtlException
    {
        public OperationExecutionException(IProcess process, IOperation operation, IRow row, string message)
            : base(process, message)
        {
            Data.Add("Operation", operation.Name);
            Data.Add("Row", row.ToDebugString());
        }

        public OperationExecutionException(IProcess process, IOperation operation, IRow row, string message, Exception innerException)
            : base(process, message, innerException)
        {
            Data.Add("Operation", operation.Name);
            Data.Add("Row", row.ToDebugString());
        }

        public OperationExecutionException(IProcess process, IOperation operation, string message, Exception innerException)
            : base(process, message, innerException)
        {
            Data.Add("Operation", operation.Name);
        }
    }
}