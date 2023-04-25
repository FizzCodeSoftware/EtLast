namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class ThrowExceptionTask : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        throw new Exception("this was really unexpected");
    }
}