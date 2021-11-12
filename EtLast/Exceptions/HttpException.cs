namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class HttpException : EtlException
    {
        public HttpException(IProcess process, string message, string url)
            : base(process, message)
        {
            Data.Add("Url", url);
        }

        public HttpException(IProcess process, string message, string url, Exception innerException)
            : base(process, message, innerException)
        {
            Data.Add("Url", url);
        }
    }
}