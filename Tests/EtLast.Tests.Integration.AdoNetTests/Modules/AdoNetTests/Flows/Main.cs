namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class Main : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .OnSuccess(() => new CreateDatabase())
            .OnSuccess(() => new GetTableMaxValueTests())
            .OnSuccess(() => new StoredProcedureAdoNetDbReaderTests())
            .OnSuccess(() => new LoadCountries())
            .OnSuccess(() => new LoadThenInsertCountries())
            .OnSuccess(() => new MergeOnlyInsertCountries())
            .OnSuccess(() => new MergeUpdateCountries())
            .OnSuccess(() => new CreatePrimaryKeyConstraintTests())
            .OnSuccess(() => new CustomSqlAdoNetDbReaderTests())
            .OnSuccess(() => new CopyTableIntoExistingTableTests())
            .OnSuccess(() => new CopyTableIntoNewTableTests())
            .OnSuccess(() => new DropDatabase())
            .HandleErrorIsolated(ctx => new DropDatabase())
            .ThrowOnError();
    }
}