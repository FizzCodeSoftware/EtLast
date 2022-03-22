namespace FizzCode.EtLast.ConsoleHost.SerilogSink
{
    using System.Collections.Generic;

    internal static class CustomColoredProperties
    {
        internal static Dictionary<string, ColorCode> Map { get; } = new Dictionary<string, ColorCode>()
        {
            //["Module"] = ColorCode.Module,
            //["Plugin"] = ColorCode.Plugin,
            ["ActiveTopic"] = ColorCode.Topic,
            ["ActiveProcess"] = ColorCode.Process,
            ["Caller"] = ColorCode.Process,
            ["Process"] = ColorCode.Process,
            ["ActiveTask"] = ColorCode.Task,
            ["Task"] = ColorCode.Task,
            ["InputProcess"] = ColorCode.Process,
            ["Operation"] = ColorCode.Operation,
            ["Job"] = ColorCode.Job,
            ["Transaction"] = ColorCode.Transaction,
            ["ConnectionStringName"] = ColorCode.SourceOrTarget,
            ["TableName"] = ColorCode.SourceOrTarget,
            ["TableNames"] = ColorCode.SourceOrTarget,
            ["SchemaName"] = ColorCode.SourceOrTarget,
            ["SchemaNames"] = ColorCode.SourceOrTarget,
            ["SourceTableName"] = ColorCode.SourceOrTarget,
            ["TargetTableName"] = ColorCode.SourceOrTarget,
            ["FileName"] = ColorCode.SourceOrTarget,
            ["SourceFileName"] = ColorCode.SourceOrTarget,
            ["TargetFileName"] = ColorCode.SourceOrTarget,
            ["Folder"] = ColorCode.SourceOrTarget,
            ["Path"] = ColorCode.SourceOrTarget,
            ["SourcePath"] = ColorCode.SourceOrTarget,
            ["TargetPath"] = ColorCode.SourceOrTarget,
            ["Url"] = ColorCode.SourceOrTarget,
            ["SourceUrl"] = ColorCode.SourceOrTarget,
            ["TargetUrl"] = ColorCode.SourceOrTarget,
        };
    }
}