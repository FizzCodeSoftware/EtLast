namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    internal sealed class FluentProcessMutatorBuilder : IFluentProcessMutatorBuilder
    {
        public IFluentProcessBuilder ProcessBuilder { get; }
        internal RowTestDelegate AutomaticallySetIfFilter { get; set; }
        internal RowTagTestDelegate AutomaticallySetTagFilter { get; set; }

        internal FluentProcessMutatorBuilder(IFluentProcessBuilder parent)
        {
            ProcessBuilder = parent;
        }

        public IFluentProcessMutatorBuilder AddMutator(IMutator mutator)
        {
            if (AutomaticallySetIfFilter != null)
                mutator.If = AutomaticallySetIfFilter;

            if (AutomaticallySetTagFilter != null)
                mutator.TagFilter = AutomaticallySetTagFilter;

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

        public IFluentProcessMutatorBuilder If(RowTestDelegate rowTester, Action<IFluentProcessMutatorBuilder> builder)
        {
            var tempBuilder = new FluentProcessMutatorBuilder(ProcessBuilder)
            {
                AutomaticallySetIfFilter = rowTester,
            };

            builder.Invoke(tempBuilder);

            return this;
        }

        public IFluentProcessMutatorBuilder IfTag(RowTagTestDelegate tagTester, Action<IFluentProcessMutatorBuilder> builder)
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