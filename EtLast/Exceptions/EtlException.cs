namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using System.Security;
    using System.Text;

    [ComVisible(true)]
    [Serializable]
    public class EtlException : Exception
    {
        public static readonly string OpsMessageDataKey = "OpsMessage";

        public EtlException(string message)
            : base(message)
        {
            var frame = Array.Find(new StackTrace().GetFrames(), sf => !sf.GetMethod().IsConstructor);
            if (frame != null)
                Data.Add("Caller", FrameToString(frame));
        }

        public EtlException(string message, Exception innerException)
            : base(message, innerException)
        {
            var frame = Array.Find(new StackTrace().GetFrames(), sf => !sf.GetMethod().IsConstructor);
            if (frame != null)
                Data.Add("Caller", FrameToString(frame));
        }

        public EtlException(IProcess process, string message)
            : base(message)
        {
            var frame = Array.Find(new StackTrace().GetFrames(), sf => !sf.GetMethod().IsConstructor);
            if (frame != null)
                Data.Add("Caller", FrameToString(frame));

            Data.Add("Process", process.Name);
            Data.Add("CallChain", GetCallChain(process));
        }

        public EtlException(IProcess process, string message, Exception innerException)
            : base(message, innerException)
        {
            var frame = Array.Find(new StackTrace().GetFrames(), sf => !sf.GetMethod().IsConstructor);
            if (frame != null)
                Data.Add("Caller", FrameToString(frame));

            Data.Add("Process", process.Name);
            Data.Add("CallChain", GetCallChain(process));
        }

        private static string FrameToString(StackFrame frame)
        {
            var sb = new StringBuilder(200);

            var method = frame.GetMethod();
            if (method == null)
                return "<unknown method>";

            if (method.DeclaringType != null)
            {
                sb.Append(method.DeclaringType.GetFriendlyTypeName())
                    .Append(".");
            }

            sb.Append(method.Name);

            if (method is MethodInfo mi && mi.IsGenericMethod)
            {
                sb.Append("<")
                    .Append(string.Join(",", mi.GetGenericArguments().Select(TypeHelpers.GetFriendlyTypeName)))
                    .Append(">");
            }

            sb.Append("(")
                .Append(string.Join(", ", method.GetParameters().Select(mp => mp.ParameterType.GetFriendlyTypeName() + " " + mp.Name)))
                .Append(")");

            try
            {
                var fileName = frame.GetFileName();
                if (frame.GetNativeOffset() != -1 && fileName != null)
                {
                    sb.AppendFormat(CultureInfo.InvariantCulture, " in {0}, line {1}", fileName, frame.GetFileLineNumber());
                }
            }
            catch (NotSupportedException)
            {
            }
            catch (SecurityException)
            {
            }

            return sb.ToString();
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