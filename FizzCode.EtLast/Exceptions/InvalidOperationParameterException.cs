namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;
    using System.Runtime.Serialization;

    [ComVisible(true)]
    [Serializable]
    public class InvalidOperationParameterException : EtlException
    {
        public static string ValueCannotBeNullMessage = "value cannot be null or empty";

        public InvalidOperationParameterException(IBaseOperation operation, string parameterName, object value, string cause)
            : base(operation.Process, "invalid operation parameter")
        {
            Data.Add("Operation", operation.Name);
            Data.Add("Parameter", parameterName);
            Data.Add("Value", value != null ? value.ToString() : "NULL");
            Data.Add("Cause", cause);
        }

        protected InvalidOperationParameterException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}