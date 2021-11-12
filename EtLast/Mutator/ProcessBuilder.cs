namespace FizzCode.EtLast
{
    public sealed class ProcessBuilder : IProcessBuilder
    {
        public IEvaluable InputProcess { get; set; }
        public MutatorList Mutators { get; set; }

        public IEvaluable Build()
        {
            if (Mutators == null || Mutators.Count == 0)
            {
                if (InputProcess == null)
                    throw new InvalidParameterException(nameof(ProcessBuilder), nameof(InputProcess), null, "When " + nameof(InputProcess) + " is not specified then at least one mutator must be specified");

                return InputProcess;
            }

            var last = InputProcess;
            foreach (var list in Mutators)
            {
                if (list != null)
                {
                    foreach (var mutator in list)
                    {
                        if (mutator != null)
                        {
                            mutator.InputProcess = last;
                            last = mutator;
                        }
                    }
                }
            }

            return last;
        }

        public static IFluentProcessBuilder Fluent => new FluentProcessBuilder();
    }
}