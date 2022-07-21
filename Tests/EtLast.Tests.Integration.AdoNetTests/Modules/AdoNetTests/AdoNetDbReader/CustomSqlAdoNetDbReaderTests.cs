namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public class CustomSqlAdoNetDbReaderTests : AbstractEtlTask
{
    public NamedConnectionString ConnectionString { get; init; }

    public override void ValidateParameters()
    {
        if (ConnectionString == null)
            throw new ProcessParameterNullException(this, nameof(ConnectionString));
    }

    public override IEnumerable<IJob> CreateJobs()
    {
        yield return new CustomJob(Context)
        {
            Name = "CustomSqlAdoNetDbReader",
            Action = job =>
            {
                var result = SequenceBuilder.Fluent
                .ReadFromCustomSql(new CustomSqlAdoNetDbReader(Context)
                {
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
