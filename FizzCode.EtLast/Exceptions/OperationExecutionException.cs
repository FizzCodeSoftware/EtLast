namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;
    using System.Runtime.Serialization;

    [ComVisible(true)]
    [Serializable]
    public class OperationExecutionException : EtlException
    {
        public OperationExecutionException(IProcess process, IBaseOperation operation, IRow row, string message)
            : base(process, message)
        {
            Data.Add("Operation", operation.Name);
            Data.Add("Row", row.ToDebugString());
        }

        public OperationExecutionException(IProcess process, IBaseOperation operation, IRow row, string message, Exception innerException)
            : base(process, message, innerException)
        {
            Data.Add("Operation", operation.Name);
            Data.Add("Row", row.ToDebugString());
        }

        public OperationExecutionException(IProcess process, IBaseOperation operation, string message, Exception innerException)
            : base(process, message, innerException)
        {
            Data.Add("Operation", operation.Name);
        }

        protected OperationExecutionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}