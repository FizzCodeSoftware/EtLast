namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public class Main : AbstractEtlFlow
{
    public override void ValidateParameters()
    {
    }

    public override void Execute()
    {
        Session.ExecuteTask(this, new EtlRunInfoTest());
        Session.ExecuteTask(this, new EtlRunInfoOptimizedTest());
        Session.ExecuteTask(this, new History1Test());
        Session.ExecuteTask(this, new History2Test());
        Session.ExecuteTask(this, new History3Test());
        Session.ExecuteTask(this, new NullValidityTest());
        Session.ExecuteTask(this, new EtlRunIdForDefaultValidFromTest());
    }
}
