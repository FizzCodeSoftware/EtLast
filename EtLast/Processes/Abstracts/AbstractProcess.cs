﻿using System.Reflection;

namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractProcess : IProcess
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    public ProcessExecutionInfo ExecutionInfo { get; set; }

    public IEtlContext Context => FlowState?.Context;

    public FlowState FlowState { get; private set; }
    public FlowState GetFlowState() => FlowState;

    public string Name { get; set; }
    public string Kind { get; }

    public LogSeverity PublicSettablePropertyLogSeverity { get; init; } = LogSeverity.Verbose;
    public LogSeverity CallLogSeverity { get; init; } = LogSeverity.Debug;

    protected AbstractProcess()
    {
        Name = GetType().GetFriendlyTypeName();
        Kind = this switch
        {
            IEtlTask => "task",
            IRowSource => "source",
            IRowSink => "sink",
            IMutator => "mutator",
            IScope => "scope",
            ISequence => "sequence",
            IProcess => "process",
            _ => "unknown",
        };
    }

    protected void LogCall(ICaller caller)
    {
        var typeName = GetType().GetFriendlyTypeName();
        if (Name == typeName)
        {
            if (caller is IEtlTask asTask)
                Context.Log(CallLogSeverity, this, "{ProcessKind} started by {Task}", Kind, asTask.UniqueName);
            else if (caller is IProcess asProcess)
                Context.Log(CallLogSeverity, this, "{ProcessKind} started by {Process}", Kind, asProcess.UniqueName);
            else
                Context.Log(CallLogSeverity, this, "{ProcessKind} started", Kind);
        }
        else
        {
            if (caller is IEtlTask asTask)
                Context.Log(CallLogSeverity, this, "{ProcessType}/{ProcessKind} started by {Task}", typeName, Kind, asTask.UniqueName);
            else if (caller is IProcess asProcess)
                Context.Log(CallLogSeverity, this, "{ProcessType}/{ProcessKind} started by {Process}", typeName, Kind, asProcess.UniqueName);
            else
                Context.Log(CallLogSeverity, this, "{ProcessType}/{ProcessKind} started", typeName, Kind);
        }
    }

    protected void LogResult(Stopwatch netTimeStopwatch)
    {
        netTimeStopwatch.Stop();
        Context.RegisterProcessEnd(this, netTimeStopwatch.ElapsedMilliseconds);

        if (ExecutionInfo.Timer.Elapsed.TotalMilliseconds >= Context.ElapsedMillisecondsLimitToLog)
        {
            Context.Log(CallLogSeverity, this, "{ProcessResult} in {Elapsed}/{ElapsedWallClock}",
                FlowState.StatusToLogString(), ExecutionInfo.Timer.Elapsed, netTimeStopwatch.Elapsed);
        }
        else
        {
            Context.Log(CallLogSeverity, this, "{ProcessResult}",
                FlowState.StatusToLogString());
        }
    }

    public override string ToString()
    {
        var typeName = GetType().GetFriendlyTypeName();
        return typeName + (Name != typeName ? " (" + Name + ")" : "");
    }

    private void LogPublicSettableProperties()
    {
        var baseProperties = typeof(AbstractEtlTask).GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public | BindingFlags.DeclaredOnly)
            .Concat(typeof(AbstractProcess).GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public | BindingFlags.DeclaredOnly))
            .Concat(typeof(AbstractMutator).GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public | BindingFlags.DeclaredOnly))
            .Select(x => x.Name)
            .ToHashSet();

        var properties = GetType().GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public)
            .Where(p => p.SetMethod?.IsPublic == true && !baseProperties.Contains(p.Name) && p.GetIndexParameters().Length == 0)
            .ToList();

        foreach (var property in properties)
        {
            var value = property.GetValue(this);
            Context.Log(PublicSettablePropertyLogSeverity, this, "parameter [{ParameterName}] = {ParameterValue}",
                property.Name, ValueFormatter.Default.Format(value, CultureInfo.InvariantCulture) ?? "<NULL>");
        }
    }

    protected void LogPrivateSettableProperties(LogSeverity severity)
    {
        var baseProperties = typeof(AbstractEtlTask).GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public | BindingFlags.DeclaredOnly)
            .Concat(typeof(AbstractProcess).GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public | BindingFlags.DeclaredOnly))
            .Concat(typeof(AbstractMutator).GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public | BindingFlags.DeclaredOnly))
            .Select(x => x.Name)
            .ToHashSet();

        var properties = GetType().GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public)
            .Where(p => p.SetMethod?.IsPublic == false && !baseProperties.Contains(p.Name) && p.GetIndexParameters().Length == 0)
            .ToList();

        foreach (var property in properties)
        {
            var value = property.GetValue(this);
            Context.Log(severity, this, "output [{ParameterName}] = {ParameterValue}",
                property.Name, ValueFormatter.Default.Format(value, CultureInfo.InvariantCulture) ?? "<NULL>");
        }
    }

    public abstract void Execute(ICaller caller, FlowState flowState = null);

    protected void BeginExecution(ICaller caller, FlowState flowState, bool overwriteArguments = false)
    {
        ArgumentNullException.ThrowIfNull(caller);
        FlowState = flowState ?? caller.FlowState;
        Context.RegisterProcessStart(this, caller);
        LogCall(caller);

        if (Context.Arguments != null)
        {
            try
            {
                var baseProperties = typeof(AbstractEtlTask).GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public | BindingFlags.DeclaredOnly)
                    .Concat(typeof(AbstractProcess).GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public | BindingFlags.DeclaredOnly))
                    .Concat(typeof(AbstractMutator).GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public | BindingFlags.DeclaredOnly))
                    .Select(x => x.Name)
                    .ToHashSet();

                Context.Arguments.Inject(this, Name, baseProperties, overwriteArguments);
            }
            catch (Exception ex)
            {
                flowState.AddException(this, ex);
            }
        }

        LogPublicSettableProperties();
    }

    public void ValidateParameterAnnotations()
    {
        ValidateParameterAnnotations(this, this);
    }

    public static void ValidateParameterAnnotations(IProcess process, object instance)
    {
        var properties = instance.GetType().GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public)
          .Where(p => p.SetMethod?.IsPrivate == false && p.GetIndexParameters().Length == 0)
          .ToList();

        foreach (var property in properties)
        {
            var value = property.GetValue(instance);
            if (property.GetCustomAttribute<ProcessParameterMustHaveValueAttribute>() is ProcessParameterMustHaveValueAttribute attr)
            {
                if (value == null)
                {
                    throw new ProcessParameterNullException(process, property.Name);
                }
                else if (attr.ThrowOnEmptyString && value is string str)
                {
                    if (string.IsNullOrEmpty(str))
                        throw new ProcessParameterNullException(process, property.Name);
                }
                else if (attr.ThrowOnZeroIntegralNumeric && value is sbyte sbyteValue)
                {
                    if (sbyteValue == 0)
                        throw new ProcessParameterNullException(process, property.Name);
                }
                else if (attr.ThrowOnZeroIntegralNumeric && value is byte byteValue)
                {
                    if (byteValue == 0)
                        throw new ProcessParameterNullException(process, property.Name);
                }
                else if (attr.ThrowOnZeroIntegralNumeric && value is short shortValue)
                {
                    if (shortValue == 0)
                        throw new ProcessParameterNullException(process, property.Name);
                }
                else if (attr.ThrowOnZeroIntegralNumeric && value is ushort ushortValue)
                {
                    if (ushortValue == 0)
                        throw new ProcessParameterNullException(process, property.Name);
                }
                else if (attr.ThrowOnZeroIntegralNumeric && value is int intValue)
                {
                    if (intValue == 0)
                        throw new ProcessParameterNullException(process, property.Name);
                }
                else if (attr.ThrowOnZeroIntegralNumeric && value is uint uintValue)
                {
                    if (uintValue == 0)
                        throw new ProcessParameterNullException(process, property.Name);
                }
                else if (attr.ThrowOnZeroIntegralNumeric && value is long longValue)
                {
                    if (longValue == 0)
                        throw new ProcessParameterNullException(process, property.Name);
                }
                else if (attr.ThrowOnZeroIntegralNumeric && value is ulong ulongValue)
                {
                    if (ulongValue == 0)
                        throw new ProcessParameterNullException(process, property.Name);
                }
                else if (attr.ThrowOnZeroIntegralNumeric && value is nint nintValue)
                {
                    if (nintValue == 0)
                        throw new ProcessParameterNullException(process, property.Name);
                }
                else if (attr.ThrowOnZeroIntegralNumeric && value is nuint nuintValue)
                {
                    if (nuintValue == 0)
                        throw new ProcessParameterNullException(process, property.Name);
                }
                else if (attr.ThrowOnEmptyArray && value is Array arr)
                {
                    if (arr.Length == 0)
                        throw new ProcessParameterNullException(process, property.Name);
                }
                else if (attr.ThrowOnEmptyCollection && value is ICollection coll)
                {
                    if (coll.Count == 0)
                        throw new ProcessParameterNullException(process, property.Name);
                }
                else if (attr.ThrowOnYearOneDate && value is DateOnly dateOnly)
                {
                    if (dateOnly.Year == 1 && dateOnly.Month == 1 && dateOnly.Day == 1)
                        throw new ProcessParameterNullException(process, property.Name);
                }
                else if (attr.ThrowOnYearOneDate && value is DateTime dateTime)
                {
                    if (dateTime.Year == 1 && dateTime.Month == 1 && dateTime.Day == 1)
                        throw new ProcessParameterNullException(process, property.Name);
                }
            }

            if (value != null && (property.PropertyType.GetCustomAttribute<ContainsProcessParameterValidationAttribute>() != null
                || value.GetType().GetCustomAttribute<ContainsProcessParameterValidationAttribute>() != null))
            {
                // support: IEnumerable<T> here

                ValidateParameterAnnotations(process, value);
            }
        }
    }

    public virtual void ValidateParameters()
    {
    }
}