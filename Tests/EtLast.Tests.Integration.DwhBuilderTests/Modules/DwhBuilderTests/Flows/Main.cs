namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class Main : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .ExecuteProcess(() => new EtlRunInfoTest())
            .ExecuteProcess(() => new EtlRunInfoOptimizedTest())
            .ExecuteProcess(() => new History1Test())
            .ExecuteProcess(() => new History2Test())
            .ExecuteProcess(() => new History3Test())
            .ExecuteProcess(() => new NullValidityTest())
            .ExecuteProcess(() => new EtlRunIdForDefaultValidFromTest());
    }
}