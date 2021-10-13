namespace FizzCode.EtLast
{
    public interface IEtlPlugin
    {
        IEtlSession Session { get; }
        IEtlContext Context { get; }
        ITopic PluginTopic { get; }
        string Name { get; }

        void Init(ITopic topic, IEtlSession session);
        void BeforeExecute();
        void Execute();
    }
}