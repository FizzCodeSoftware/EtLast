namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;

    [ComVisible(true)]
    [Serializable]
    public class StreamReadException : EtlException
    {
        public StreamReadException(IProcess process, string message, NamedStream stream)
            : base(process, message)
        {
            Data.Add("StreamName", stream.Name);
        }

        public StreamReadException(IProcess process, string message, NamedStream stream, Exception innerException)
            : base(process, message, innerException)
        {
            Data.Add("StreamName", stream.Name);
        }
    }
}