namespace FizzCode.EtLast.PluginHost
{
    using System.Collections.Generic;
    using System.Linq;

    public class EtlSession : IEtlSession
    {
        public EtlModuleConfiguration CurrentModuleConfiguration { get; private set; }
        public IEtlPlugin CurrentPlugin { get; private set; }
        public string Id { get; }
        private readonly List<IEtlService> _services = new();

        public EtlSession(string id)
        {
            Id = id;
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

        internal void ModuleChanged(EtlModuleConfiguration configuration)
        {
            CurrentModuleConfiguration = configuration;

            var servicesToRemove = _services.Where(x => x.Lifespan != EtlServiceLifespan.PerSession);
            foreach (var service in servicesToRemove)
            {
                service.Stop();
                _services.Remove(service);
            }
        }

        internal void PluginChanged(IEtlPlugin plugin)
        {
            CurrentPlugin = plugin;

            var servicesToRemove = _services.Where(x => x.Lifespan == EtlServiceLifespan.PerPlugin);
            foreach (var service in servicesToRemove)
            {
                service.Stop();
                _services.Remove(service);
            }
        }

        internal void Stop()
        {
            foreach (var service in _services)
            {
                service.Stop();
            }

            _services.Clear();
        }
    }
}