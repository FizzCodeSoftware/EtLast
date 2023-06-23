namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class Main : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .ContinueWithProcess(() => new EtlRunInfoTest())
            .ContinueWithProcess(() => new EtlRunInfoOptimizedTest())
            .ContinueWithProcess(() => new History1Test())
            .ContinueWithProcess(() => new History2Test())
            .ContinueWithProcess(() => new History3Test())
            .ContinueWithProcess(() => new NullValidityTest())
            .ContinueWithProcess(() => new EtlRunIdForDefaultValidFromTest());
    }
}