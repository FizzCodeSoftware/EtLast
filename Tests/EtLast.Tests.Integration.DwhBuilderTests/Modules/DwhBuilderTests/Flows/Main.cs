namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class Main : AbstractEtlFlow
{
    public override void ValidateParameters()
    {
    }

    public override void Execute()
    {
        ExecuteTask(new EtlRunInfoTest());
        ExecuteTask(new EtlRunInfoOptimizedTest());
        ExecuteTask(new History1Test());
        ExecuteTask(new History2Test());
        ExecuteTask(new History3Test());
        ExecuteTask(new NullValidityTest());
        ExecuteTask(new EtlRunIdForDefaultValidFromTest());
    }
}