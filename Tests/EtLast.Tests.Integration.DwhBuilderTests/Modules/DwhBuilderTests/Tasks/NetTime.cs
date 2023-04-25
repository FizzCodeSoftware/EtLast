namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class NetTime : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .OnSuccess(() => CreateSequence(1))
            .OnSuccess(() => CreateSequence(100))
            .OnSuccess(() => CreateSequence(10000));
    }

    private ISequence CreateSequence(int mod)
    {
        return SequenceBuilder.Fluent
            .ImportEnumerable(new EnumerableImporter(Context)
            {
                Name = "SlowSource",
                InputGenerator = process => DelayedInputGenerator(process, 1000000 / mod, mod),
            })
            .CustomCode("QuickMutator", row => true)
            .CustomCode("SlowMutator", row =>
            {
                ExpensiveStuff(mod);
                return true;
            })
            .Build();
    }

    private IEnumerable<IReadOnlySlimRow> DelayedInputGenerator(EnumerableImporter process, int count, int mod)
    {
        for (var i = 1; i <= count; i++)
        {
            ExpensiveStuff(mod);

            yield return new SlimRow()
            {
                ["index"] = i,
            };
        }
    }

    private void ExpensiveStuff(int mod)
    {
        var sum = 0;
        var n = 10000 * mod;
        for (var i = 0; i < n; i++)
            sum = (int)(((long)sum + i) % int.MaxValue);
    }
}