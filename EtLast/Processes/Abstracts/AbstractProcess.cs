using System.Reflection;

namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractProcess : IProcess
{
    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public ProcessInvocationInfo InvocationInfo { get; set; }

    public IEtlContext Context => FlowState.Context;

    public FlowState FlowState { get; private set; }
    public FlowState GetFlowState() => FlowState;

    public string Name { get; set; }
    public string Kind { get; }

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
        var severity = this is IEtlTask or IScope
            ? LogSeverity.Information
            : LogSeverity.Debug;

        var typeName = GetType().GetFriendlyTypeName();
        if (Name == typeName)
        {
            if (caller is IEtlTask asTask)
                Context.Log(severity, this, "{ProcessKind} started by {Task}", Kind, asTask.InvocationName);
            else if (caller is IProcess asProcess)
                Context.Log(severity, this, "{ProcessKind} started by {Process}", Kind, asProcess.InvocationName);
            else
                Context.Log(severity, this, "{ProcessKind} started", Kind);
        }
        else
        {
            if (caller is IEtlTask asTask)
                Context.Log(severity, this, "{ProcessType}/{ProcessKind} started by {Task}", typeName, Kind, asTask.InvocationName);
            else if (caller is IProcess asProcess)
                Context.Log(severity, this, "{ProcessType}/{ProcessKind} started by {Process}", typeName, Kind, asProcess.InvocationName);
            else
                Context.Log(severity, this, "{ProcessType}/{ProcessKind} started", typeName, Kind);
        }
    }

    protected void LogResult(Stopwatch netTimeStopwatch)
    {
        var severity = this is IEtlTask or IScope
            ? LogSeverity.Information
            : LogSeverity.Debug;

        netTimeStopwatch.Stop();
        Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);

        if (InvocationInfo.InvocationStarted.Elapsed.TotalMilliseconds >= Context.ElapsedMillisecondsLimitToLog)
        {
            Context.Log(severity, this, "{ProcessResult} in {Elapsed}/{ElapsedWallClock}",
                FlowState.StatusToLogString(), InvocationInfo.InvocationStarted.Elapsed, netTimeStopwatch.Elapsed);
        }
        else
        {
            Context.Log(severity, this, "{ProcessResult}",
                FlowState.StatusToLogString());
        }
    }

    public override string ToString()
    {
        var typeName = GetType().GetFriendlyTypeName();
        return typeName + (Name != typeName ? " (" + Name + ")" : "");
    }

    public virtual string GetTopic()
    {
        return null;
    }

    protected void LogPublicSettableProperties(LogSeverity severity)
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
            Context.Log(severity, this, "parameter [{ParameterName}] = {ParameterValue}",
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
        FlowState = flowState ?? caller.GetFlowState();
        Context.RegisterProcessInvocationStart(this, caller);
        LogCall(caller);

        if (Context.Arguments == null)
            return;

        var baseProperties = typeof(AbstractEtlTask).GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public | BindingFlags.DeclaredOnly)
            .Concat(typeof(AbstractProcess).GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public | BindingFlags.DeclaredOnly))
            .Concat(typeof(AbstractMutator).GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public | BindingFlags.DeclaredOnly))
            .Select(x => x.Name)
            .ToHashSet();

        var properties = GetType().GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public)
            .Where(p => p.SetMethod != null && p.SetMethod.IsPrivate != true && !baseProperties.Contains(p.Name) && p.GetIndexParameters().Length == 0)
            .ToList();

        foreach (var property in properties)
        {
            if (!overwriteArguments && property.GetValue(this) != null)
                continue;

            var key = Context.Arguments.AllKeys.FirstOrDefault(x => string.Equals(x, property.Name, StringComparison.InvariantCultureIgnoreCase));
            key ??= Context.Arguments.AllKeys.FirstOrDefault(x => string.Equals(x, Name + ":" + property.Name, StringComparison.InvariantCultureIgnoreCase));

            if (key != null)
            {
                var value = Context.Arguments.Get(key);
                if (value != null && property.PropertyType.IsAssignableFrom(value.GetType()))
                    property.SetValue(this, value);
            }
        }
    }

    public void ValidateParameterAnnotations()
    {
        ValidateParameterAnnotations(this, this);
    }

    public static void ValidateParameterAnnotations(IProcess process, object instance)
    {
        var properties = instance.GetType().GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public)
          .Where(p => p.SetMethod != null && p.SetMethod.IsPrivate != true && p.GetIndexParameters().Length == 0)
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