namespace FizzCode.EtLast;

[ContainsProcessParameterValidation]
public class OneMemoryStreamProvider : IOneStreamProvider, IManyStreamProvider
{
    [ProcessParameterMustHaveValue]
    public required MemoryStream Stream { get; init; }

    public string Name { get; init; } = "MemoryStream";

    public string GetTopic()
    {
        return Name;
    }

    public NamedStream GetStream(IProcess caller)
    {
        var ioCommand = caller.Context.RegisterIoCommand(new IoCommand()
        {
            Process = caller,
            Kind = IoCommandKind.streamRead,
            Message = "reading from memory stream"
        });

        return new NamedStream(Name, Stream, ioCommand);
    }

    public IEnumerable<NamedStream> GetStreams(IProcess caller)
    {
        var stream = GetStream(caller);
        return new[] { stream };
    }
}