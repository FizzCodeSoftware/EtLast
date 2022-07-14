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
        Session.ExecuteTask(this, new StoredProcedureAdoNetDbReader());
        Session.ExecuteTask(this, new ResilientSqlScope());

        Session.ExecuteTask(this, new DropDatabase());
    }
}
