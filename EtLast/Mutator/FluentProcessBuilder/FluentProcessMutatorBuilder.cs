namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    internal sealed class FluentProcessMutatorBuilder : IFluentProcessMutatorBuilder
    {
        public IFluentProcessBuilder ProcessBuilder { get; }
        internal RowTagTestDelegate AutomaticallySetTagFilter { get; set; }

        internal FluentProcessMutatorBuilder(IFluentProcessBuilder parent)
        {
            ProcessBuilder = parent;
        }

        public IFluentProcessMutatorBuilder AddMutator(IMutator mutator)
        {
            if (AutomaticallySetTagFilter != null)
            {
                mutator.TagFilter = AutomaticallySetTagFilter;
            }

            mutator.InputProcess = ProcessBuilder.Result;
            ProcessBuilder.Result = mutator;
            return this;
        }

        public IFluentProcessMutatorBuilder AddMutators(IEnumerable<IMutator> mutators)
        {
            foreach (var mutator in mutators)
            {
                if (AutomaticallySetTagFilter != null)
                {
                    mutator.TagFilter = AutomaticallySetTagFilter;
                }

                mutator.InputProcess = ProcessBuilder.Result;
                ProcessBuilder.Result = mutator;
            }

            return this;
        }

        public IFluentProcessMutatorBuilder OnBranch(RowTagTestDelegate tagTester, Action<IFluentProcessMutatorBuilder> builder)
        {
            var tempBuilder = new FluentProcessMutatorBuilder(ProcessBuilder)
            {
                AutomaticallySetTagFilter = tagTester,
            };

            builder.Invoke(tempBuilder);

            return this;
        }

        public IProducer Build()
        {
            return ProcessBuilder.Build();
        }
    }
}