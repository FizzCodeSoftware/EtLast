namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class Main : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .ContinueWith(() => new EtlRunInfoTest())
            .ContinueWith(() => new EtlRunInfoOptimizedTest())
            .ContinueWith(() => new History1Test())
            .ContinueWith(() => new History2Test())
            .ContinueWith(() => new History3Test())
            .ContinueWith(() => new NullValidityTest())
            .ContinueWith(() => new EtlRunIdForDefaultValidFromTest());
    }
}