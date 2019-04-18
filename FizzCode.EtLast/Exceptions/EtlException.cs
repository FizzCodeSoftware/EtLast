namespace FizzCode.EtLast
{
    using System;
    using System.Runtime.InteropServices;
    using System.Runtime.Serialization;

    [ComVisible(true)]
    [Serializable]
    public class EtlException : Exception
    {
        public static readonly string OpsMessageDataKey = "OpsMessage";

        public EtlException(IProcess process, string message)
            : base(message)
        {
            Data.Add("Process", process.Name);
            Data.Add("CallChain", GetCallChain(process));
        }

        public EtlException(IProcess process, string message, Exception innerException)
            : base(message, innerException)
        {
            Data.Add("Process", process.Name);
            Data.Add("CallChain", GetCallChain(process));
        }

        protected EtlException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        private static string GetCallChain(IProcess process)
        {
            var callChain = process.Name;
            var p = process.Caller;
            while (p != null)
            {
                callChain = p.Name + " -> " + callChain;
                p = p.Caller;
            }

            return "| -> " + callChain;
        }

        public void AddOpsMessage(string message)
        {
            Data.Add(OpsMessageDataKey, message);
        }
    }
}