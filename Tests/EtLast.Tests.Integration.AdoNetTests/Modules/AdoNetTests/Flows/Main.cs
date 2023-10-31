namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class Main : AbstractEtlTask
{
    public override void Execute(IFlow flow)
    {
        flow
            .ExecuteProcess(() => new CreateDatabase())
            .ExecuteProcess(() => new GetTableMaxValueTests())
            .ExecuteProcess(() => new StoredProcedureAdoNetDbReaderTests())
            .ExecuteProcess(() => new LoadCountries())
            .ExecuteProcess(() => new LoadThenInsertCountries())
            .ExecuteProcess(() => new MergeOnlyInsertCountries())
            .ExecuteProcess(() => new MergeUpdateCountries())
            .ExecuteProcess(() => new CreatePrimaryKeyConstraintTests())
            .ExecuteProcess(() => new CustomSqlAdoNetDbReaderTests())
            .ExecuteProcess(() => new CopyTableIntoExistingTableTests())
            .ExecuteProcess(() => new CopyTableIntoNewTableTests())
            .ExecuteProcess(() => new DropDatabase())
            .HandleError(() => new DropDatabase())
            .ThrowOnError();
    }
}