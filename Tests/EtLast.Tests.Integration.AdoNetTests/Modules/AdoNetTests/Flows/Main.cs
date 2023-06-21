namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class Main : AbstractEtlTask
{
    public override void ValidateParameters()
    {
    }

    public override void Execute(IFlow flow)
    {
        flow
            .ContinueWith(() => new CreateDatabase())
            .ContinueWith(() => new GetTableMaxValueTests())
            .ContinueWith(() => new StoredProcedureAdoNetDbReaderTests())
            .ContinueWith(() => new LoadCountries())
            .ContinueWith(() => new LoadThenInsertCountries())
            .ContinueWith(() => new MergeOnlyInsertCountries())
            .ContinueWith(() => new MergeUpdateCountries())
            .ContinueWith(() => new CreatePrimaryKeyConstraintTests())
            .ContinueWith(() => new CustomSqlAdoNetDbReaderTests())
            .ContinueWith(() => new CopyTableIntoExistingTableTests())
            .ContinueWith(() => new CopyTableIntoNewTableTests())
            .ContinueWith(() => new DropDatabase())
            .HandleError(ctx => new DropDatabase())
            .ThrowOnError();
    }
}