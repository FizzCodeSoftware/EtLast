using System.Reflection;

namespace FizzCode.EtLast;

public sealed class EtlSession : IEtlSession
{
    public string Id { get; }
    public IEtlContext Context { get; }
    private readonly EtlSessionArguments _arguments;

    public bool Success { get; private set; }

    private readonly List<IEtlService> _services = new();

    public EtlSession(string id, EtlContext context, EtlSessionArguments arguments)
    {
        Id = id;
        Context = context;
        _arguments = arguments;
    }

    public T Service<T>() where T : IEtlService, new()
    {
        var service = _services.OfType<T>().FirstOrDefault();
        if (service != null)
            return service;

        service = new T();
        service.Start(this);
        _services.Add(service);

        return service;
    }

    public void Stop()
    {
        foreach (var service in _services)
        {
            service.Stop();
        }

        _services.Clear();
    }

    public TaskResult<T> ExecuteTask<T>(IProcess caller, T task)
        where T : IEtlTask
    {
        SetPublicSettableProperiesFromArguments(task);

        var result = task.Execute(caller, this);
        Success = result.ExceptionCount == 0;

        return new TaskResult<T>()
        {
            ExceptionCount = result.ExceptionCount,
            Task = task,
        };
    }

    public ProcessResult ExecuteProcess(IProcess caller, IExecutable process)
    {
        if (process is IEtlTask)
            throw new ArgumentException("For executing tasks, use " + nameof(ExecuteTask) + " instead.", nameof(process));

        var originalExceptionCount = Context.ExceptionCount;
        process.Execute(caller);
        var result = new ProcessResult()
        {
            ExceptionCount = Context.ExceptionCount - originalExceptionCount,
        };

        Success = result.ExceptionCount == 0;
        return result;
    }

    private void SetPublicSettableProperiesFromArguments(IProcess process)
    {
        var baseProperties = typeof(AbstractEtlTask).GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public | BindingFlags.DeclaredOnly)
            .Concat(typeof(AbstractProcess).GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public | BindingFlags.DeclaredOnly))
            .Select(x => x.Name)
            .ToHashSet();

        var properties = process.GetType().GetProperties(BindingFlags.Instance | BindingFlags.SetProperty | BindingFlags.Public)
            .Where(p => p.SetMethod?.IsPublic == true && !baseProperties.Contains(p.Name))
            .ToList();

        foreach (var property in properties)
        {
            if (property.GetValue(process) != null)
                continue;

            var argument = _arguments.All.FirstOrDefault(x => string.Equals(x.Key, property.Name, StringComparison.InvariantCultureIgnoreCase));
            if (argument.Key == null)
            {
                argument = _arguments.All.FirstOrDefault(x => string.Equals(x.Key, process.Name + ":" + property.Name, StringComparison.InvariantCultureIgnoreCase));
            }

            if (argument.Key != null)
            {
                var value = argument.Value;
                if (value != null && value is Func<object> func)
                {
                    value = func.Invoke();
                }

                if (value != null && value is Func<IEtlSessionArguments, object> funcWithArgs)
                {
                    value = funcWithArgs.Invoke(_arguments);
                }

                if (value != null && property.PropertyType.IsAssignableFrom(value.GetType()))
                {
                    property.SetValue(process, value);
                }
                else
                {
                }
            }
        }
    }
}
