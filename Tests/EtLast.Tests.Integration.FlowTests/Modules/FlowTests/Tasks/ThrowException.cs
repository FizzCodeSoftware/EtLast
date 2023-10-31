namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class ThrowException : AbstractEtlTask
{
    public override void Execute(IFlow flow)
    {
        throw new Exception("this was really unexpected");
    }
}