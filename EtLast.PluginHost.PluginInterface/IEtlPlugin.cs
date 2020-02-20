namespace FizzCode.EtLast
{
    public interface IEtlPlugin
    {
        ModuleConfiguration ModuleConfiguration { get; }
        IEtlContext Context { get; }
        ITopic PluginTopic { get; }
        string Name { get; }

        void Init(ITopic topic, ModuleConfiguration moduleConfiguration);
        void BeforeExecute();
        void Execute();
    }
}