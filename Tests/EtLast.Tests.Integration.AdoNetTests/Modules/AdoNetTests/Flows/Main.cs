namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class Main : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .ContinueWithProcess(() => new CreateDatabase())
            .ContinueWithProcess(() => new GetTableMaxValueTests())
            .ContinueWithProcess(() => new StoredProcedureAdoNetDbReaderTests())
            .ContinueWithProcess(() => new LoadCountries())
            .ContinueWithProcess(() => new LoadThenInsertCountries())
            .ContinueWithProcess(() => new MergeOnlyInsertCountries())
            .ContinueWithProcess(() => new MergeUpdateCountries())
            .ContinueWithProcess(() => new CreatePrimaryKeyConstraintTests())
            .ContinueWithProcess(() => new CustomSqlAdoNetDbReaderTests())
            .ContinueWithProcess(() => new CopyTableIntoExistingTableTests())
            .ContinueWithProcess(() => new CopyTableIntoNewTableTests())
            .ContinueWithProcess(() => new DropDatabase())
            .HandleError(() => new DropDatabase())
            .ThrowOnError();
    }
}