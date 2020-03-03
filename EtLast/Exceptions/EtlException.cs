namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
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
            var frame = Array.Find(new StackTrace(true).GetFrames(), sf => !sf.GetMethod().IsConstructor && !sf.GetMethod().IsStatic);
            if (frame != null)
                Data.Add("Caller", FrameToString(frame));
        }

        public EtlException(string message, Exception innerException)
            : base(message, innerException)
        {
            var frame = Array.Find(new StackTrace(true).GetFrames(), sf => !sf.GetMethod().IsConstructor && !sf.GetMethod().IsStatic);
            if (frame != null)
                Data.Add("Caller", FrameToString(frame));
        }

        public EtlException(IProcess process, string message)
            : base(message)
        {
            var frame = Array.Find(new StackTrace(true).GetFrames(), sf => !sf.GetMethod().IsConstructor && !sf.GetMethod().IsStatic);
            if (frame != null)
                Data.Add("Caller", FrameToString(frame));

            Data.Add("Process", process.Name);
            Data.Add("CallChain", GetCallChain(process));
        }

        public EtlException(IProcess process, string message, Exception innerException)
            : base(message, innerException)
        {
            var frame = Array.Find(new StackTrace(true).GetFrames(), sf => !sf.GetMethod().IsConstructor && !sf.GetMethod().IsStatic);
            if (frame != null)
                Data.Add("Caller", FrameToString(frame));

            Data.Add("Process", process.Name);
            Data.Add("CallChain", GetCallChain(process));
        }

        public static string FrameToString(StackFrame frame)
        {
            var sb = new StringBuilder(200);

            var method = frame.GetMethod();
            if (method == null)
                return "<unknown method>";

            var ignoreMethod = false;

            if (!method.Name.StartsWith("<", StringComparison.Ordinal) && method.DeclaringType != null)
            {
                if (method.DeclaringType.Name.StartsWith("<", StringComparison.Ordinal))
                {
                    var endIndex = method.DeclaringType.Name.IndexOf('>', StringComparison.Ordinal);
                    if (endIndex > -1 && endIndex < method.DeclaringType.Name.Length)
                    {
                        switch (method.DeclaringType.Name[endIndex + 1])
                        {
                            case 'd':
                                sb.Append(TypeHelpers.FixGeneratedName(method.DeclaringType.DeclaringType.Name))
                                    .Append(".");
                                ignoreMethod = true;
                                break;
                        }
                    }
                }

                sb.Append(TypeHelpers.FixGeneratedName(method.DeclaringType.Name));
                if (!ignoreMethod)
                    sb.Append(".");
            }

            if (!ignoreMethod)
            {
                sb.Append(TypeHelpers.FixGeneratedName(method.Name));

                if (method is MethodInfo mi && mi.IsGenericMethod)
                {
                    sb.Append("<")
                        .Append(string.Join(",", mi.GetGenericArguments().Select(TypeHelpers.GetFriendlyTypeName)))
                        .Append(">");
                }

                sb.Append("(")
                    .Append(string.Join(", ", method.GetParameters().Select(mp => mp.ParameterType.GetFriendlyTypeName() + " " + mp.Name)))
                    .Append(")");
            }

            try
            {
                var fileName = frame.GetFileName();
                if (frame.GetNativeOffset() != -1 && fileName != null)
                {
                    sb.AppendFormat(CultureInfo.InvariantCulture, " in {0}, line {1}", Path.GetFileName(fileName), frame.GetFileLineNumber());
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
            var p = process.InvocationInfo?.Caller;
            while (p != null)
            {
                callChain = p.Name + " -> " + callChain;
                p = p.InvocationInfo?.Caller;
            }

            return "| -> " + callChain;
        }

        public void AddOpsMessage(string message)
        {
            Data.Add(OpsMessageDataKey, message);
        }
    }
}