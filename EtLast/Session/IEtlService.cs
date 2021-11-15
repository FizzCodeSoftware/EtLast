namespace FizzCode.EtLast
{
    public interface IEtlService
    {
        public IEtlSession Session { get; }
        public void Start(IEtlSession session);
        public void Stop();
    }
}