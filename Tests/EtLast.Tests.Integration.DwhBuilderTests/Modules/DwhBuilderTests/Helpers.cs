namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests;

public static class Helpers
{
    public static DateTime EtlRunId1 { get; } = new DateTime(2001, 1, 1, 1, 1, 1, DateTimeKind.Utc);
    public static DateTime EtlRunId2 { get; } = new DateTime(2022, 2, 2, 2, 2, 2, DateTimeKind.Utc);

    public static List<ISlimRow> ReadRows(IProcess caller, NamedConnectionString connectionString, string schema, string table)
    {
        return new AdoNetDbReader(caller.Context)
        {
            Name = "Reader",
            ConnectionString = connectionString,
            TableName = connectionString.Escape(table, schema),
        }.Evaluate(caller).TakeRowsAndReleaseOwnership().ToList();
    }
}
