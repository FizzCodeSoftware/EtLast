namespace FizzCode.EtLast
{
    public interface IEtlPlugin
    {
        ModuleConfiguration ModuleConfiguration { get; }
        IEtlContext Context { get; }
        string Name { get; }

        void Init(IEtlContext context, ModuleConfiguration moduleConfiguration);
        void BeforeExecute();
        void Execute();
    }
}