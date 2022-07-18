namespace FizzCode.EtLast;

public sealed class ProcessBuilder : IProcessBuilder
{
    public IProducer InputJob { get; set; }
    public MutatorList Mutators { get; set; }

    public IProducer Build()
    {
        if (Mutators == null || Mutators.Count == 0)
        {
            if (InputJob == null)
                throw new InvalidParameterException(nameof(ProcessBuilder), nameof(InputJob), null, "When " + nameof(InputJob) + " is not specified then at least one mutator must be specified");

            return InputJob;
        }

        var last = InputJob;
        foreach (var list in Mutators)
        {
            if (list != null)
            {
                foreach (var mutator in list)
                {
                    if (mutator != null)
                    {
                        mutator.Input = last;
                        last = mutator;
                    }
                }
            }
        }

        return last;
    }

    public static IFluentProcessBuilder Fluent => new FluentProcessBuilder();
}
