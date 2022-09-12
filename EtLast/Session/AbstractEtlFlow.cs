using System.Reflection;

namespace FizzCode.EtLast;

public abstract class AbstractEtlFlow : AbstractProcess, IEtlFlow
{
    public IEtlSession Session { get; private set; }

    private readonly ExecutionStatistics _statistics = new();
    public IExecutionStatistics Statistics => _statistics;

    public Dictionary<IoCommandKind, IoCommandCounter> IoCommandCounters => _ioCommandCounterCollection.Counters;

    private readonly IoCommandCounterCollection _ioCommandCounterCollection = new();

    public abstract void Execute();

    protected AbstractEtlFlow()
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
        _statistics.Start();

        try
        {
            ValidateParameters();
        }
        catch (Exception ex)
        {
            InvocationContext.AddException(this, ex);
        }

        if (!InvocationContext.IsTerminating)
        {
            Context.Listeners.Add(_ioCommandCounterCollection);
            try
            {
                Execute();
            }
            catch (Exception ex)
            {
                InvocationContext.AddException(this, ex);
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

        netTimeStopwatch.Stop();
        Context.RegisterProcessInvocationEnd(this, netTimeStopwatch.ElapsedMilliseconds);
    }

    public T ExecuteTask<T>(T task)
        where T : IEtlTask
    {
        task.SetArguments(Session.Arguments);

        var taskInvocationContext = new ProcessInvocationContext(Context);
        task.Execute(this, Session, taskInvocationContext);
        return task;
    }

    public T ExecuteJob<T>(T job)
        where T : IJob
    {
        var jobInvocationContext = new ProcessInvocationContext(Context);
        job.Execute(this, jobInvocationContext);
        return job;
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

            var argument = arguments.All.FirstOrDefault(x => string.Equals(x.Key, property.Name, StringComparison.InvariantCultureIgnoreCase));
            if (argument.Key == null)
            {
                argument = arguments.All.FirstOrDefault(x => string.Equals(x.Key, Name + ":" + property.Name, StringComparison.InvariantCultureIgnoreCase));
            }

            if (argument.Key != null)
            {
                var value = argument.Value;
                if (value is Func<object> func)
                {
                    value = func.Invoke();
                }

                if (value is Func<IArgumentCollection, object> funcWithArgs)
                {
                    value = funcWithArgs.Invoke(arguments);
                }

                if (value != null && property.PropertyType.IsAssignableFrom(value.GetType()))
                {
                    property.SetValue(this, value);
                }
            }
        }
    }
}