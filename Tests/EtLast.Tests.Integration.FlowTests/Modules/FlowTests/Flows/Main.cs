namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class Main : AbstractEtlFlow
{
    public override void ValidateParameters()
    {
    }

    public override void Execute()
    {
        ExecuteTask(new ExceptionInFlowTest());
    }
}