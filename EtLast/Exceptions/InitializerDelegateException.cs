namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class InitializerDelegateException : EtlException
    {
        public InitializerDelegateException(IProcess process, Exception innerException)
            : base(process, "error during the initialization of the process", innerException)
        {
        }
    }
}