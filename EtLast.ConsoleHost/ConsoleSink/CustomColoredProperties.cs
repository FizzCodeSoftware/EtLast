namespace FizzCode.EtLast.ConsoleHost.SerilogSink;

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
        ["ConnectionStringName"] = ColorCode.Location,
        ["TableName"] = ColorCode.Location,
        ["TableNames"] = ColorCode.Location,
        ["SchemaName"] = ColorCode.Location,
        ["SchemaNames"] = ColorCode.Location,
        ["SourceTableName"] = ColorCode.Location,
        ["TargetTableName"] = ColorCode.Location,
        ["FileName"] = ColorCode.Location,
        ["SourceFileName"] = ColorCode.Location,
        ["TargetFileName"] = ColorCode.Location,
        ["Folder"] = ColorCode.Location,
        ["Path"] = ColorCode.Location,
        ["SourcePath"] = ColorCode.Location,
        ["TargetPath"] = ColorCode.Location,
        ["Url"] = ColorCode.Location,
        ["SourceUrl"] = ColorCode.Location,
        ["TargetUrl"] = ColorCode.Location,
        ["Container"] = ColorCode.Location,
        ["SourceContainer"] = ColorCode.Location,
        ["TargetContainer"] = ColorCode.Location,
        ["Pattern"] = ColorCode.Location,
        ["SourcePattern"] = ColorCode.Location,
        ["TargetPattern"] = ColorCode.Location,
    };
}
