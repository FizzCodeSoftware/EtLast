namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public sealed class ResilientSqlScopeProcessBuilder
    {
        public ResilientSqlScope Scope { get; init; }
        public List<IExecutable> Processes { get; } = new List<IExecutable>();

        public ResilientSqlScopeProcessBuilder Add(IExecutable process)
        {
            Processes.Add(process);

            return this;
        }
    }
}