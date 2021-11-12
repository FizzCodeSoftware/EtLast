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
            var trace = GetTraceFromStackFrames(new StackTrace(true).GetFrames());
            if (trace != null)
                Data.Add("Trace", trace);
        }

        public EtlException(string message, Exception innerException)
            : base(message, innerException)
        {
            var trace = GetTraceFromStackFrames(new StackTrace(true).GetFrames());
            if (trace != null)
                Data.Add("Trace", trace);
        }

        public EtlException(IProcess process, string message)
            : base(message)
        {
            var trace = GetTraceFromStackFrames(new StackTrace(true).GetFrames());
            if (trace != null)
                Data.Add("Trace", trace);

            Data.Add("ProcessName", process.Name);
            if (process.Topic?.Name != null)
                Data.Add("ProcessTopic", process.Topic.Name);

            Data.Add("ProcessType", process.GetType().GetFriendlyTypeName());

            var assembly = process.GetType().Assembly?.GetName()?.Name;
            if (assembly != null)
                Data.Add("ProcessTypeAssembly", assembly);

            Data.Add("ProcessKind", process.Kind.ToString());

            Data.Add("CallChain", GetCallChain(process));
        }

        public EtlException(IProcess process, string message, Exception innerException)
            : base(message, innerException)
        {
            var trace = GetTraceFromStackFrames(new StackTrace(true).GetFrames());
            if (trace != null)
                Data.Add("Trace", trace);

            Data.Add("ProcessName", process.Name);
            if (process.Topic?.Name != null)
                Data.Add("ProcessTopic", process.Topic.Name);

            Data.Add("ProcessType", process.GetType().GetFriendlyTypeName());

            var assembly = process.GetType().Assembly?.GetName()?.Name;
            if (assembly != null)
                Data.Add("ProcessTypeAssembly", assembly);

            Data.Add("ProcessKind", process.Kind.ToString());

            Data.Add("CallChain", GetCallChain(process));
        }

        public static string GetTraceFromStackFrames(StackFrame[] frames)
        {
            if (frames == null || frames.Length == 0)
                return null;

            var ctorsFiltered = false;
            var currentFrameAdded = false;
            //var mainAssembyName = typeof(IEtlContext).Assembly.GetName().Name;
            var builder = new StringBuilder();

            foreach (var frame in frames)
            {
                if (!ctorsFiltered)
                {
                    if (frame.GetMethod().IsConstructor || frame.GetMethod().IsStatic)
                        continue;

                    ctorsFiltered = true;
                }

                if (!currentFrameAdded)
                {
                    builder.AppendLine(FrameToString(frame));
                    currentFrameAdded = true;
                    continue;
                }

                var method = frame.GetMethod();
                if (method == null)
                    continue;

                var assemblyName = method.DeclaringType?.Assembly?.GetName()?.Name;
                if (assemblyName != null)
                {
                    if (assemblyName.Equals("CommandDotNet", StringComparison.OrdinalIgnoreCase))
                        continue;

                    if (assemblyName.Equals("FizzCode.EtLast.PluginHost", StringComparison.OrdinalIgnoreCase))
                        continue;

                    try
                    {
                        var fileName = frame.GetFileName();
                        if (string.IsNullOrEmpty(fileName))
                            continue;
                    }
                    catch (NotSupportedException)
                    {
                        continue;
                    }
                    catch (SecurityException)
                    {
                        continue;
                    }
                }

                builder.AppendLine(FrameToString(frame));
            }

            return builder.ToString().Trim();
        }

        public static string FrameToString(StackFrame frame)
        {
            var sb = new StringBuilder(200);

            var method = frame.GetMethod();
            if (method == null)
                return "<unknown method>";

            var ignoreMethod = false;

            var assemblyName = method.DeclaringType?.Assembly?.GetName().Name;
            if (assemblyName != null)
            {
                sb.Append('(').Append(assemblyName).Append(") ");
            }

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
                                    .Append('.');
                                ignoreMethod = true;
                                break;
                        }
                    }
                }

                sb.Append(TypeHelpers.FixGeneratedName(method.DeclaringType.Name));
                if (!ignoreMethod)
                    sb.Append('.');
            }

            if (!ignoreMethod)
            {
                sb.Append(TypeHelpers.FixGeneratedName(method.Name));

                if (method is MethodInfo mi && mi.IsGenericMethod)
                {
                    sb.Append('<')
                        .AppendJoin(",", mi.GetGenericArguments().Select(x => x.GetFriendlyTypeName(false)))
                        .Append('>');
                }

                sb.Append('(')
                    .AppendJoin(", ", method.GetParameters().Select(mp => mp.ParameterType.GetFriendlyTypeName() + " " + mp.Name))
                    .Append(')');
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
            var builder = new StringBuilder(200);

            var p = process.InvocationInfo?.Caller;
            while (p != null)
            {
                var assemblyName = p.GetType().Assembly?.GetName().Name;
                if (assemblyName != null)
                {
                    builder.Append('(').Append(assemblyName).Append(") ");
                }

                var typeName = p.GetType().GetFriendlyTypeName();
                builder.Append(typeName);

                if (p.Name != typeName)
                {
                    builder.Append(" (\"");
                    builder.Append(p.Name);
                    builder.Append("\")");
                }

                if (p.Topic?.Name != null)
                {
                    builder.Append(", topic: ");
                    builder.Append(p.Topic.Name);
                }

                builder.Append(", kind: ");
                builder.AppendLine(p.Kind.ToString());

                p = p.InvocationInfo?.Caller;
            }

            return builder.ToString().Trim();
        }

        public void AddOpsMessage(string message)
        {
            Data.Add(OpsMessageDataKey, message);
        }
    }
}