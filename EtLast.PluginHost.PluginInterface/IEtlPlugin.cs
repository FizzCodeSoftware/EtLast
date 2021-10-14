namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IEtlPlugin
    {
        IEtlSession Session { get; }
        IEtlContext Context { get; }
        ITopic PluginTopic { get; }
        string Name { get; }

        void Init(ITopic topic, IEtlSession session);
        void BeforeExecute();

        public bool TerminateHostOnFail { get; }

        IEnumerable<IExecutable> CreateExecutables();
    }
}