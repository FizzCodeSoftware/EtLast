﻿using System.Reflection;

namespace FizzCode.EtLast;

[ComVisible(true)]
[Serializable]
public class EtlException : Exception
{
    public static readonly string OpsMessageDataKey = "OpsMessage";

    public EtlException(IProcess process, string message)
      : base(message)
    {
        var trace = GetTraceFromStackFrames(new StackTrace(true).GetFrames());
        if (trace != null)
            Data["Trace"] = trace;

        Data["ProcessName"] = process.Name;

        var topic = process.GetTopic();
        if (topic != null)
            Data["ProcessTopic"] = topic;

        Data["ProcessType"] = process.GetType().GetFriendlyTypeName();

        var assembly = process.GetType().Assembly?.GetName()?.Name;
        if (assembly != null)
            Data["ProcessTypeAssembly"] = assembly;

        Data["ProcessKind"] = process.Kind;

        Data["CallChain"] = GetCallChain(process);
    }

    public EtlException(IProcess process, string message, Exception innerException)
        : base(message + " (" + innerException.Message + ")", innerException)
    {
        var trace = GetTraceFromStackFrames(new StackTrace(true).GetFrames());
        if (trace != null)
            Data["Trace"] = trace;

        Data["ProcessName"] = process.Name;
        var topic = process.GetTopic();
        if (topic != null)
            Data["ProcessTopic"] = topic;

        Data["ProcessType"] = process.GetType().GetFriendlyTypeName();

        var assembly = process.GetType().Assembly?.GetName()?.Name;
        if (assembly != null)
            Data["ProcessTypeAssembly"] = assembly;

        Data["ProcessKind"] = process.Kind;

        Data["CallChain"] = GetCallChain(process);
    }

    public static string GetTraceFromStackFrames(StackFrame[] frames)
    {
        if (frames == null || frames.Length == 0)
            return null;

        var ctorsFiltered = false;
        var currentFrameAdded = false;
        var builder = new StringBuilder();

        var maxAssemblyNameLength = frames.Max(frame =>
        {
            var method = frame.GetMethod();
            if (method == null)
                return 0;

            return (method.DeclaringType?.Assembly?.GetName().Name)?.Length ?? 0;
        });

        foreach (var frame in frames)
        {
            var method = frame.GetMethod();
            if (method == null)
                continue;

            var assemblyName = method.DeclaringType?.Assembly?.GetName().Name;

            if (!ctorsFiltered)
            {
                if (frame.GetMethod().IsConstructor || frame.GetMethod().IsStatic)
                    continue;

                ctorsFiltered = true;
            }

            if (method.Name == nameof(FlowState.AddException))
                continue;

            if (!currentFrameAdded)
            {
                builder.AppendLine(FrameToString(frame, method, assemblyName, maxAssemblyNameLength));
                currentFrameAdded = true;
                continue;
            }

            if (assemblyName != null)
            {
                if (assemblyName.StartsWith("FizzCode.EtLast.CommandService", StringComparison.OrdinalIgnoreCase))
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

            builder.AppendLine(FrameToString(frame, method, assemblyName, maxAssemblyNameLength));
        }

        return builder.ToString().Trim();
    }

    private static string FrameToString(StackFrame frame, MethodBase method, string assemblyName, int maxAssemblyNameLength)
    {
        var sb = new StringBuilder(200);

        var ignoreMethod = false;

        if (assemblyName != null)
        {
            sb.Append("in ").Append(assemblyName.PadRight(maxAssemblyNameLength + 1)).Append(": ");
        }

        if (!method.Name.StartsWith('<') && method.DeclaringType != null)
        {
            if (method.DeclaringType.Name.StartsWith('<'))
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

        var p = process.ExecutionInfo?.Caller as IProcess;
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

            var topic = p.GetTopic();
            if (topic != null)
            {
                builder.Append(", topic: ");
                builder.Append(topic);
            }

            builder.Append(", kind: ");
            builder.AppendLine(p.Kind);

            p = p.ExecutionInfo?.Caller as IProcess;
        }

        return builder.ToString().Trim();
    }

    public void AddOpsMessage(string message)
    {
        Data.Add(OpsMessageDataKey, message);
    }
}
