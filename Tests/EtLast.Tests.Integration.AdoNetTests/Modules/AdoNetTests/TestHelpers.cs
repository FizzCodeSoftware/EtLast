﻿namespace FizzCode.EtLast.Tests.Integration.Modules.AdoNetTests;

public static class TestHelpers
{
    public static CustomJob CreateReadSqlTableAndAssertExactMatch(IAdoNetSqlConnectionString connectionString, string table, params Dictionary<string, object>[] expectedRows)
    {
        var expectedRowsList = new List<Dictionary<string, object>>(expectedRows);
        return new CustomJob()
        {
            Name = "ReadAndCheck" + table + "Table",
            Action = job => ReadSqlTableAndAssertExactMacth(job, connectionString, table, expectedRowsList)
        };
    }

    public static void ReadSqlTableAndAssertExactMacth(IProcess caller, IAdoNetSqlConnectionString connectionString, string table, List<Dictionary<string, object>> expectedRows)
    {
        var rows = ReadRows(caller, connectionString, table);
        Assert.That.ExactMatch(rows, expectedRows);
    }

    public static List<ISlimRow> ReadRows(IProcess caller, IAdoNetSqlConnectionString connectionString, string table)
    {
        return ReadRows(caller, connectionString, null, table);
    }

    public static List<ISlimRow> ReadRows(IProcess caller, IAdoNetSqlConnectionString connectionString, string schema, string table)
    {
        return new AdoNetDbReader()
        {
            Name = "Reader",
            ConnectionString = connectionString,
            TableName = connectionString.Escape(table, schema),
        }.TakeRowsAndReleaseOwnership(caller).ToList();
    }
}
