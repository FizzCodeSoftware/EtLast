namespace FizzCode.EtLast
{
    public interface IEtlSession
    {
        public EtlModuleConfiguration CurrentModuleConfiguration { get; }
        public IEtlPlugin CurrentPlugin { get; }

        public T Service<T>() where T : IEtlService, new();
    }
}