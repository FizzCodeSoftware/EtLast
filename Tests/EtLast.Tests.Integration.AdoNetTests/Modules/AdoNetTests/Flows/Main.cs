namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class Main : AbstractEtlFlow
{
    public override void ValidateParameters()
    {
    }

    public override void Execute()
    {
        NewPipe()
            .StartWith(new CreateDatabase())
            .OnSuccess(pipe => new GetTableMaxValueTests())
            .OnSuccess(pipe => new StoredProcedureAdoNetDbReaderTests())
            .OnSuccess(pipe => new LoadCountries())
            .OnSuccess(pipe => new LoadThenInsertCountries())
            .OnSuccess(pipe => new MergeOnlyInsertCountries())
            .OnSuccess(pipe => new MergeUpdateCountries())
            .OnSuccess(pipe => new CreatePrimaryKeyConstraintTests())
            .OnSuccess(pipe => new CustomSqlAdoNetDbReaderTests())
            .OnSuccess(pipe => new CopyTableIntoExistingTableTests())
            .OnSuccess(pipe => new CopyTableIntoNewTableTests())
            .OnSuccess(pipe => new DropDatabase())
            .OnError(pipe => new DropDatabase())
            .ThrowOnError();
    }
}