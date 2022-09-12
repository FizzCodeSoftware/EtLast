namespace FizzCode.EtLast;

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public abstract class AbstractProcess : IProcess
{
    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public ProcessInvocationInfo InvocationInfo { get; set; }

    public IEtlContext Context { get; protected set; }
    public ProcessInvocationContext InvocationContext { get; protected set; }
    public bool Success => InvocationContext?.IsTerminating != true;

    public string Name { get; set; }
    public string Kind { get; }

    /// <summary>
    ///  Reserved for lazy-initialized <see cref="AbstractEtlFlow"/> and <see cref="AbstractEtlTask"/> types.
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

    private static string GetProcessKind(IProcess process)
    {
        if (process.GetType().GetInterfaces().Any(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IJobWithResult<>)))
            return "jobWithResult";

        return process switch
        {
            IEtlFlow => "flow",
            IEtlTask => "task",
            IRowSource => "source",
            IRowSink => "sink",
            IMutator => "mutator",
            IScope => "scope",
            ISequence => "sequence",
            IJob => "job",
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
        var baseProperties = typeof(AbstractEtlTask).GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.SetProperty | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.DeclaredOnly)
            .Concat(typeof(AbstractProcess).GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.SetProperty | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.DeclaredOnly))
            .Select(x => x.Name)
            .ToHashSet();

        var properties = GetType().GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.SetProperty | System.Reflection.BindingFlags.Public)
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
        var baseProperties = typeof(AbstractEtlTask).GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.SetProperty | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.DeclaredOnly)
            .Concat(typeof(AbstractProcess).GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.SetProperty | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.DeclaredOnly))
            .Select(x => x.Name)
            .ToHashSet();

        var properties = GetType().GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.SetProperty | System.Reflection.BindingFlags.Public)
            .Where(p => p.SetMethod?.IsPublic == false && !baseProperties.Contains(p.Name))
            .ToList();

        foreach (var property in properties)
        {
            var value = property.GetValue(this);
            Context.Log(severity, this, "output [{ParameterName}] = {ParameterValue}",
                property.Name, ValueFormatter.Default.Format(value, CultureInfo.InvariantCulture) ?? "<NULL>");
        }
    }
}