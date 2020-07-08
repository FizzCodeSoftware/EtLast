namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IFluentProcessBuilder
    {
        IEvaluable Result { get; set; }
        FluentProcessMutatorBuilder SetInput(IEvaluable process);
    }

    public class FluentProcessBuilder : IFluentProcessBuilder
    {
        public IEvaluable Result { get; set; }

        public FluentProcessMutatorBuilder SetInput(IEvaluable process)
        {
            Result = process;
            return new FluentProcessMutatorBuilder(this);
        }
    }

    public interface IFluentProcessMutatorBuilder
    {
        IFluentProcessBuilder ProcessBuilder { get; }
        IFluentProcessMutatorBuilder AddMutators(IEnumerable<IMutator> mutators);
        IFluentProcessBuilder FinishAddingMutators();
    }

    public class FluentProcessMutatorBuilder : IFluentProcessMutatorBuilder
    {
        public IFluentProcessBuilder ProcessBuilder { get; }

        internal FluentProcessMutatorBuilder(IFluentProcessBuilder parent)
        {
            ProcessBuilder = parent;
        }

        public IFluentProcessMutatorBuilder AddMutators(IEnumerable<IMutator> mutators)
        {
            foreach (var mutator in mutators)
            {
                mutator.InputProcess = ProcessBuilder.Result;
                ProcessBuilder.Result = mutator;
            }

            return this;
        }

        public IFluentProcessBuilder FinishAddingMutators()
        {
            return ProcessBuilder;
        }
    }
}