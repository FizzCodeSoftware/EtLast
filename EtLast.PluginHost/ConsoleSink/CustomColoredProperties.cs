namespace FizzCode.EtLast.PluginHost.SerilogSink
{
    using System.Collections.Generic;

    internal static class CustomColoredProperties
    {
        internal static Dictionary<string, ColorCode> Map { get; } = new Dictionary<string, ColorCode>()
        {
            ["Module"] = ColorCode.Module,
            ["Plugin"] = ColorCode.Plugin,
            ["Caller"] = ColorCode.Process,
            ["Process"] = ColorCode.Process,
            ["InputProcess"] = ColorCode.Process,
            ["Operation"] = ColorCode.Operation,
            ["Job"] = ColorCode.Job,
            ["Transaction"] = ColorCode.Transaction,
            ["ConnectionStringKey"] = ColorCode.SourceOrTarget,
            ["TableName"] = ColorCode.SourceOrTarget,
            ["TableNames"] = ColorCode.SourceOrTarget,
            ["SchemaName"] = ColorCode.SourceOrTarget,
            ["SchemaNames"] = ColorCode.SourceOrTarget,
            ["SourceTableName"] = ColorCode.SourceOrTarget,
            ["TargetTableName"] = ColorCode.SourceOrTarget,
            ["FileName"] = ColorCode.SourceOrTarget,
            ["Folder"] = ColorCode.SourceOrTarget,
        };
    }
}