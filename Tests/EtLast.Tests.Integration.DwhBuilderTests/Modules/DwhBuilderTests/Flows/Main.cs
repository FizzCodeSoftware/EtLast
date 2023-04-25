namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class Main : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .OnSuccess(() => new EtlRunInfoTest())
            .OnSuccess(() => new EtlRunInfoOptimizedTest())
            .OnSuccess(() => new History1Test())
            .OnSuccess(() => new History2Test())
            .OnSuccess(() => new History3Test())
            .OnSuccess(() => new NullValidityTest())
            .OnSuccess(() => new EtlRunIdForDefaultValidFromTest());
    }
}