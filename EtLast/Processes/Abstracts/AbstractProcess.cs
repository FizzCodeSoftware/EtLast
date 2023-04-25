using System.Reflection;

namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractProcess : IProcess
{
    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public ProcessInvocationInfo InvocationInfo { get; set; }

    public IEtlContext Context { get; protected set; }

    public FlowState FlowState { get; protected set; }
    public bool Success => FlowState?.IsTerminating != true;

    public string Name { get; set; }
    public string Kind { get; }

    /// <summary>
    ///  Reserved for lazy-initialized <see cref="AbstractEtlTask"/> types.
    /// </summary>
    internal AbstractProcess()
    {
        Name = GetType().GetFriendlyTypeName();
        Kind = GetProcessKind(this);
    }

    protected AbstractProcess(IEtlContext context)
    {
        Context = context ?? throw new ProcessParameterNullException(this, nameof(context));
        Name = GetType().GetFriendlyTypeName();
        Kind = GetProcessKind(this);
    }

    protected void LogCall(IProcess caller)
    {
        var typeName = GetType().GetFriendlyTypeName();
        if (Name == typeName)
        {
            if (caller is IEtlTask)
                Context.Log(LogSeverity.Information, this, "{ProcessKind} started by {Task} INV#{TaskInvocationUid}", Kind, caller.Name, caller.InvocationInfo.InvocationUid);
            else if (caller != null)
                Context.Log(LogSeverity.Information, this, "{ProcessKind} started by {Process} INV#{ProcessInvocationUid}", Kind, caller.Name, caller.InvocationInfo.InvocationUid);
            else
                Context.Log(LogSeverity.Information, this, "{ProcessKind} started", Kind);
        }
        else
        {
            if (caller is IEtlTask)
                Context.Log(LogSeverity.Information, this, "{ProcessKind}/{ProcessType} started by {Task} INV#{TaskInvocationUid}", typeName, Kind, caller.Name, caller.InvocationInfo.InvocationUid);
            else if (caller != null)
                Context.Log(LogSeverity.Information, this, "{ProcessKind}/{ProcessType} started by {Process} INV#{ProcessInvocationUid}", typeName, Kind, caller.Name, caller.InvocationInfo.InvocationUid);
            else
                Context.Log(LogSeverity.Information, this, "{ProcessKind}/{ProcessType} started", typeName, Kind);
        }
    }

    private static string GetProcessKind(IProcess process)
    {
        return process switch
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
            .Select(x => x.Name)
            .ToHashSet();

        var properties = GetType().GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public)
            .Where(p => p.SetMethod?.IsPublic == true && !baseProperties.Contains(p.Name))
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
            .Select(x => x.Name)
            .ToHashSet();

        var properties = GetType().GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public)
            .Where(p => p.SetMethod?.IsPublic == false && !baseProperties.Contains(p.Name))
            .ToList();

        foreach (var property in properties)
        {
            var value = property.GetValue(this);
            Context.Log(severity, this, "output [{ParameterName}] = {ParameterValue}",
                property.Name, ValueFormatter.Default.Format(value, CultureInfo.InvariantCulture) ?? "<NULL>");
        }
    }

    public virtual void Execute(IProcess caller)
    {
        Execute(caller, null);
    }

    public abstract void Execute(IProcess caller, FlowState flow);

    public void SetContext(IEtlContext context, bool overwrite = false)
    {
        Context = context;

        var baseProperties = typeof(AbstractEtlTask).GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public | BindingFlags.DeclaredOnly)
            .Concat(typeof(AbstractProcess).GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public | BindingFlags.DeclaredOnly))
            .Select(x => x.Name)
            .ToHashSet();

        var properties = GetType().GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public)
            .Where(p => p.SetMethod?.IsPublic == true && !baseProperties.Contains(p.Name))
            .ToList();

        foreach (var property in properties)
        {
            if (!overwrite && property.GetValue(this) != null)
                continue;

            var key = context.Arguments.AllKeys.FirstOrDefault(x => string.Equals(x, property.Name, StringComparison.InvariantCultureIgnoreCase));
            key ??= context.Arguments.AllKeys.FirstOrDefault(x => string.Equals(x, Name + ":" + property.Name, StringComparison.InvariantCultureIgnoreCase));

            if (key != null)
            {
                var value = context.Arguments.Get(key);
                if (value != null && property.PropertyType.IsAssignableFrom(value.GetType()))
                {
                    property.SetValue(this, value);
                }
            }
        }
    }

    public virtual void ValidateParameters()
    {
    }
}