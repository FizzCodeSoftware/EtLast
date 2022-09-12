namespace FizzCode.EtLast.Tests.Integration.Modules.FlowTests;

public class ThrowExceptionFlow : AbstractEtlFlow
{
    public override void ValidateParameters()
    {
    }

    public override void Execute()
    {
        throw new Exception("this was really unexpected");
    }
}