namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class InvalidProcessParameterException : EtlException
    {
        public InvalidProcessParameterException(IProcess process, string parameterName, object value, string cause)
            : base(process, "invalid process parameter")
        {
            Data.Add("Parameter", parameterName);
            Data.Add("Value", value != null ? value.ToString() : "NULL");
            Data.Add("Cause", cause);
        }
    }
}