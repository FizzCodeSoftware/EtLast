namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class ProcessParameterNullException : InvalidProcessParameterException
    {
        public ProcessParameterNullException(IProcess block, string parameterName)
            : base(block, parameterName, null, "value cannot be null or empty")
        {
        }
    }
}