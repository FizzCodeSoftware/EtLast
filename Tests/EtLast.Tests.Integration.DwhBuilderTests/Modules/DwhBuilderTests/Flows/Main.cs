namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class Main : AbstractEtlFlow
{
    public override void ValidateParameters()
    {
    }

    public override void Execute()
    {
        NewPipe()
            .StartWith(new EtlRunInfoTest())
            .OnSuccess(pipe => new EtlRunInfoOptimizedTest())
            .OnSuccess(pipe => new History1Test())
            .OnSuccess(pipe => new History2Test())
            .OnSuccess(pipe => new History3Test())
            .OnSuccess(pipe => new NullValidityTest())
            .OnSuccess(pipe => new EtlRunIdForDefaultValidFromTest());
    }
}