namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;

    public sealed class ResilientSqlTableTableFinalizerBuilder
    {
        public ResilientTableBase Table { get; init; }
        public List<IExecutable> Finalizers { get; } = new List<IExecutable>();

        public ResilientSqlTableTableFinalizerBuilder Add(IExecutable process)
        {
            Finalizers.Add(process);
            return this;
        }
    }
}