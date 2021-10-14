namespace FizzCode.EtLast
{
    public enum EtlServiceLifespan { PerPlugin, PerModule, PerSession }

    public interface IEtlService
    {
        public EtlServiceLifespan Lifespan { get; }
        public IEtlSession Session { get; }
        public void Start(IEtlSession session);
        public void Stop();
    }
}