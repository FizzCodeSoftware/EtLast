namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public static class TestHelpers
{
    public static CustomJob CreateReadSqlTableAndAssertExactMatch(NamedConnectionString connectionString, string table, params CaseInsensitiveStringKeyDictionary<object>[] expectedRows)
    {
        var expectedRowsList = new List<CaseInsensitiveStringKeyDictionary<object>>(expectedRows);
        return new CustomJob()
        {
            Name = "ReadAndCheck" + table + "Table",
            Action = job => ReadSqlTableAndAssertExactMacth(job, connectionString, table, expectedRowsList)
        };
    }

    public static void ReadSqlTableAndAssertExactMacth(IProcess caller, NamedConnectionString connectionString, string table, List<CaseInsensitiveStringKeyDictionary<object>> expectedRows)
    {
        var rows = ReadRows(caller, connectionString, table);
        Assert.That.ExactMatch(rows, expectedRows);
    }

    public static List<ISlimRow> ReadRows(IProcess caller, NamedConnectionString connectionString, string table)
    {
        return ReadRows(caller, connectionString, null, table);
    }

    public static List<ISlimRow> ReadRows(IProcess caller, NamedConnectionString connectionString, string schema, string table)
    {
        return new AdoNetDbReader()
        {
            Name = "Reader",
            ConnectionString = connectionString,
            TableName = connectionString.Escape(table, schema),
        }.TakeRowsAndReleaseOwnership(caller).ToList();
    }
}
