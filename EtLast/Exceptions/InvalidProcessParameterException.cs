namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class InvalidProcessParameterException : EtlException
    {
        public InvalidProcessParameterException(IProcess block, string parameterName, object value, string cause)
            : base(block, "invalid parameter")
        {
            Data.Add("Parameter", parameterName);
            Data.Add("Value", value != null ? value.ToString() : "NULL");
            Data.Add("Cause", cause);
        }
    }
}