namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public sealed class EtlSession : IEtlSession
    {
        public string Id { get; }
        public IEtlContext Context { get; }

        public bool Success { get; private set; }

        private readonly List<IEtlService> _services = new();

        public EtlSession(string id, EtlContext context)
        {
            Id = id;
            Context = context;
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

        public TaskResult ExecuteTask(IProcess caller, IEtlTask task)
        {
            var taskResult = task.Execute(caller, this);
            Success = taskResult.ExceptionCount == 0;
            return taskResult;
        }

        public TaskResult ExecuteProcess(IProcess caller, IExecutable process)
        {
            var originalExceptionCount = Context.ExceptionCount;
            process.Execute(caller);
            var taskResult = new TaskResult()
            {
                ExceptionCount = Context.ExceptionCount - originalExceptionCount,
            };

            Success = taskResult.ExceptionCount == 0;
            return taskResult;
        }
    }
}