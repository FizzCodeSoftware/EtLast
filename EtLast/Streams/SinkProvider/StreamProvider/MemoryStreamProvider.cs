namespace FizzCode.EtLast;

[ContainsProcessParameterValidation]
public class MemoryStreamProvider : IStreamProvider
{
    [ProcessParameterMustHaveValue]
    public required Func<MemoryStream> StreamCreator { get; init; }

    private readonly string _streamName = "MemoryStream";

    public string GetTopic()
    {
        return _streamName;
    }

    public IEnumerable<NamedStream> GetStreams(IProcess caller)
    {
        var ioCommand = caller.Context.RegisterIoCommand(new IoCommand()
        {
            Process = caller,
            Kind = IoCommandKind.streamRead,
            Message = "reading from memory stream"
        });

        try
        {
            var stream = StreamCreator.Invoke();
            return new[]
            {
                new NamedStream(_streamName, stream, ioCommand),
            };
        }
        catch (Exception ex)
        {
            var exception = new EtlException(caller, "error while opening memory stream", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while opening memory stream: {0}, message: {1}",
                _streamName, ex.Message));
            exception.Data["StreamName"] = _streamName;

            ioCommand.Failed(exception);
            throw exception;
        }
    }
}