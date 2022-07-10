namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class Main : AbstractEtlFlow
{
    public override void ValidateParameters()
    {
    }

    public override void Execute()
    {
        Session.ExecuteTask(this, new CreateDatabase());
        Session.ExecuteTask(this, new GetTableMaxValue());
    }
}
