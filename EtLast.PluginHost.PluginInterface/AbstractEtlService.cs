namespace FizzCode.EtLast
{
    public abstract class AbstractEtlService : IEtlService
    {
        public IEtlSession Session { get; private set; }
        public abstract EtlServiceLifespan Lifespan { get; }

        public void Start(IEtlSession session)
        {
            Session = session;
            OnStart();
        }

        public void Stop()
        {
            OnStop();
        }

        protected abstract void OnStart();
        protected abstract void OnStop();
    }
}