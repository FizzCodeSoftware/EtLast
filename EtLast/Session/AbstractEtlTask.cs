using System.Reflection;

namespace FizzCode.EtLast;

public abstract class AbstractEtlTask : AbstractProcess, IEtlTask
{
    public IEtlSession Session { get; private set; }

    private readonly ExecutionStatistics _statistics = new();
    public IExecutionStatistics Statistics => _statistics;

    public Dictionary<IoCommandKind, IoCommandCounter> IoCommandCounters => _ioCommandCounterCollection.Counters;

    private readonly IoCommandCounterCollection _ioCommandCounterCollection = new();

    public abstract IEnumerable<IJob> CreateJobs();

    protected AbstractEtlTask()
    {
    }

    public abstract void ValidateParameters();

    public void Execute(IProcess caller, IEtlSession session, ProcessInvocationContext invocationContext)
    {
        Session = session;
        Context = session.Context;

        Context.RegisterProcessInvocationStart(this, caller);
        InvocationContext = invocationContext ?? caller?.InvocationContext ?? new ProcessInvocationContext(Context);

        if (caller != null)
            Context.Log(LogSeverity.Information, this, "{ProcessKind} started by {Process}", Kind, caller.Name);
        else
            Context.Log(LogSeverity.Information, this, "{ProcessKind} started", Kind);

        LogPublicSettableProperties(LogSeverity.Debug);

        var netTimeStopwatch = Stopwatch.StartNew();
        try
        {
            _statistics.Start();

            ValidateParameters();

            Context.Listeners.Add(_ioCommandCounterCollection);
            try
            {
                var jobs = CreateJobs()?
                    .Where(x => x != null)
                    .ToList();

                if (jobs?.Count > 0)
                {
                    for (var jobIndex = 0; jobIndex < jobs.Count; jobIndex++)
                    {
                        var job = jobs[jobIndex];

                        job.Execute(this);

                        if (InvocationContext.IsTerminating)
                            break;
                    }
                }
            }
            finally
            {
                Session.Context.Listeners.Remove(_ioCommandCounterCollection);
            }

            _statistics.Finish();

            Context.Log(LogSeverity.Information, this, "{ProcessKind} {TaskResult} in {Elapsed}",
                Kind, InvocationContext.ToLogString(), _statistics.RunTime);

            LogPrivateSettableProperties(LogSeverity.Debug);
        }
        finally
        {
            netTimeStopwatch.Stop();
            Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
        }
    }

    public void SetArguments(ArgumentCollection arguments)
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
            if (property.GetValue(this) != null)
                continue;

            var key = arguments.AllKeys.FirstOrDefault(x => string.Equals(x, property.Name, StringComparison.InvariantCultureIgnoreCase));
            key ??= arguments.AllKeys.FirstOrDefault(x => string.Equals(x, Name + ":" + property.Name, StringComparison.InvariantCultureIgnoreCase));

            if (key != null)
            {
                var value = arguments.Get(key);
                if (value != null && property.PropertyType.IsAssignableFrom(value.GetType()))
                {
                    property.SetValue(this, value);
                }
            }
        }
    }
}