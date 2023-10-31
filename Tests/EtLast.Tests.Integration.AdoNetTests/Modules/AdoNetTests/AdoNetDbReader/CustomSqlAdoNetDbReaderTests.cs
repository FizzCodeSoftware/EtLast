namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class CustomSqlAdoNetDbReaderTests : AbstractEtlTask
{
    [ProcessParameterMustHaveValue]
    public NamedConnectionString ConnectionString { get; init; }

    public override void Execute(IFlow flow)
    {
        flow
            .CustomJob("CheckCustomSqlResult", job =>
            {
                var result = SequenceBuilder.Fluent
                .ReadFromCustomSql(new CustomSqlAdoNetDbReader(Context)
                {
                    Name = "ReturnCustomSqlResult",
                    ConnectionString = ConnectionString,
                    MainTableName = null,
                    Sql = "SELECT 1 as Id UNION SELECT 2 as Id"
                })
                .Build().TakeRowsAndReleaseOwnership(this).ToList();

                Assert.AreEqual(2, result.Count);
                Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                    new() { ["Id"] = 1},
                    new() { ["Id"] = 2}
                });
            });
    }
}
