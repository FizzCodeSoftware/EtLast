namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class InvalidOperationParameterException : EtlException
    {
        public InvalidOperationParameterException(IBaseOperation operation, string parameterName, object value, string cause)
            : base(operation.Process, "invalid operation parameter")
        {
            Data.Add("Operation", operation.Name);
            Data.Add("Parameter", parameterName);
            Data.Add("Value", value != null ? value.ToString() : "NULL");
            Data.Add("Cause", cause);
        }
    }
}