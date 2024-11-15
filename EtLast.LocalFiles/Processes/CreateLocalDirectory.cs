﻿namespace FizzCode.EtLast;

public sealed class CreateLocalDirectory : AbstractJob
{
    [ProcessParameterMustHaveValue]
    public required string Path { get; init; }

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        try
        {
            if (!Directory.Exists(Path))
                Directory.CreateDirectory(Path);
        }
        catch (Exception ex)
        {
            var exception = new CreateDirectoryException(this, ex);
            exception.Data["Path"] = Path;
            throw exception;
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class CreateLocalDirectoryFluent
{
    public static IFlow CreateLocalDirectory(this IFlow builder, string name, string path)
    {
        return builder.ExecuteProcess(() => new CreateLocalDirectory()
        {
            Name = name,
            Path = path,
        });
    }
}