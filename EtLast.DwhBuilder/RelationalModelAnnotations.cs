namespace FizzCode.EtLast.DwhBuilder;

public static class RelationalModelAnnotations
{
    public static bool GetUsedByEtlRunInfo(this RelationalColumn column)
    {
        return column.GetFlag("EtlRunInfo");
    }

    public static RelationalColumn SetUsedByEtlRunInfo(this RelationalColumn column, bool value = true)
    {
        column.SetFlag("EtlRunInfo", value);
        return column;
    }

    public static bool GetRecordTimestampIndicator(this RelationalColumn column)
    {
        return column.GetFlag("TimestampIndicator");
    }

    public static RelationalColumn SetRecordTimestampIndicator(this RelationalColumn column, bool value = true)
    {
        column.SetFlag("TimestampIndicator", value);
        return column;
    }

    public static RelationalColumn GetRecordTimestampIndicatorColumn(this RelationalTable table)
    {
        return table.GetColumnsWithFlag("TimestampIndicator").FirstOrDefault();
    }

    public static string NameEscaped(this RelationalColumn column, NamedConnectionString connectionString)
    {
        return connectionString.Escape(column.Name);
    }

    public static string EscapedName(this RelationalTable table, NamedConnectionString connectionString)
    {
        return connectionString.Escape(table.Name, table.Schema.Name);
    }

    public static bool GetHasHistoryTable(this RelationalTable table)
    {
        return table.GetFlag("HasHistory");
    }

    public static RelationalTable SetHasHistoryTable(this RelationalTable table, bool value = true)
    {
        table.SetFlag("HasHistory", value);
        return table;
    }

    public static bool GetIsHistoryTable(this RelationalTable table)
    {
        return table.GetFlag("IsHistory");
    }

    public static RelationalTable SetIsHistoryTable(this RelationalTable table, bool value = true)
    {
        table.SetFlag("IsHistory", value);
        return table;
    }

    public static bool GetEtlRunInfoDisabled(this RelationalTable table)
    {
        return table.GetFlag("NoEtlRunInfo");
    }

    public static RelationalTable SetEtlRunInfoDisabled(this RelationalTable table, bool value = true)
    {
        table.SetFlag("NoEtlRunInfo", value);
        return table;
    }

    public static bool GetHistoryDisabled(this RelationalColumn column)
    {
        return column.GetFlag("NoHistory");
    }

    public static RelationalColumn SetHistoryDisabled(this RelationalColumn column, bool value = true)
    {
        column.SetFlag("NoHistory", value);
        return column;
    }

    public static int? GetLimitedStringLength(this RelationalColumn column)
    {
        if (column.GetAdditionalData("MaxLength") is int length)
            return length;

        return null;
    }

    public static RelationalColumn SetLimitedStringLength(this RelationalColumn column, int length)
    {
        column.SetAdditionalData("MaxLength", length);
        return column;
    }

    public static bool GetIsEtlRunInfo(this RelationalTable table)
    {
        return table.GetFlag("EtlRunInfo");
    }

    public static RelationalTable SetEtlRunInfo(this RelationalTable table, bool value = true)
    {
        table.SetFlag("EtlRunInfo", value);
        return table;
    }

    public static RelationalTable GetEtlRunInfoTable(this RelationalModel schema)
    {
        return schema.GetTablesWithFlag("EtlRunInfo").FirstOrDefault();
    }

    public static RelationalTable SetSourceTableNameOverride(this RelationalTable table, string sourceTableName)
    {
        table.SetAdditionalData("SourceTableName", sourceTableName);
        return table;
    }

    public static string GetSourceTableNameOverride(this RelationalTable table)
    {
        return table.GetAdditionalData("SourceTableName") as string;
    }
}
