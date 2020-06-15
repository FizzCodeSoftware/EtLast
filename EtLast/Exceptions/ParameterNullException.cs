namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class ParameterNullException : InvalidParameterException
    {
        public ParameterNullException(string location, string parameterName)
            : base(location, parameterName, null, "value cannot be null or empty")
        {
        }
    }
}