namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class CustomSqlAdoNetDbReaderTests : AbstractEtlTask
{
    public NamedConnectionString ConnectionString { get; init; }

    public override void ValidateParameters()
    {
        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));
    }

    public override IEnumerable<IProcess> CreateJobs(IProcess caller)
    {
        yield return new CustomJob(Context)
        {
            Name = "CheckCustomSqlResult",
            Action = job =>
            {
                var result = SequenceBuilder.Fluent
                .ReadFromCustomSql(new CustomSqlAdoNetDbReader(Context)
                {
                    Name = "ReturnCustomSqlResult",
                    ConnectionString = ConnectionString,
                    MainTableName = "none",
                    Sql = "SELECT 1 as Id UNION SELECT 2 as Id"
                })
                .Build().TakeRowsAndReleaseOwnership(this).ToList();

                Assert.AreEqual(2, result.Count);
                Assert.That.ExactMatch(result, new List<CaseInsensitiveStringKeyDictionary<object>>() {
                    new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 1},
                    new CaseInsensitiveStringKeyDictionary<object>() { ["Id"] = 2}
                });
            }
        };
    }
}
