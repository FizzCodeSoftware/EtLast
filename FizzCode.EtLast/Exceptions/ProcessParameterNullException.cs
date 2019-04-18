namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class ProcessParameterNullException : InvalidProcessParameterException
    {
        public ProcessParameterNullException(IProcess process, string parameterName)
            : base(process, parameterName, null, "value cannot be null or empty")
        {
        }
    }
}