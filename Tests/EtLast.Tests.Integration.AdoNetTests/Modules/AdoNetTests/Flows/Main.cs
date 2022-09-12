namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class Main : AbstractEtlFlow
{
    public override void ValidateParameters()
    {
    }

    public override void Execute()
    {
        ExecuteTask(new CreateDatabase());

        ExecuteTask(new GetTableMaxValueTests());
        ExecuteTask(new StoredProcedureAdoNetDbReaderTests());
        ExecuteTask(new LoadCountries());
        ExecuteTask(new LoadThenInsertCountries());
        ExecuteTask(new MergeOnlyInsertCountries());
        ExecuteTask(new MergeUpdateCountries());
        ExecuteTask(new CreatePrimaryKeyConstraintTests());
        ExecuteTask(new CustomSqlAdoNetDbReaderTests());
        ExecuteTask(new CopyTableIntoExistingTableTests());
        ExecuteTask(new CopyTableIntoNewTableTests());

        ExecuteTask(new DropDatabase());
    }
}
