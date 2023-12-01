using System.Net.Http;

namespace FizzCode.EtLast;

[ContainsProcessParameterValidation]
public class HttpStreamProvider : IStreamProvider
{
    /// <summary>
    /// According to MSDN, it is recommended to reuse HttpClient instances if possible.
    /// https://learn.microsoft.com/en-us/dotnet/fundamentals/networking/http/httpclient-guidelines
    /// </summary>
    [ProcessParameterMustHaveValue]
    public required HttpClient Client { get; init; }

    [ProcessParameterMustHaveValue]
    public required string Url { get; init; }

    /// <summary>
    /// Default value is true.
    /// </summary>
    public bool ThrowExceptionWhenFailed { get; init; } = true;

    public string GetTopic() => Url;

    public IEnumerable<NamedStream> GetStreams(IProcess caller)
    {
        var ioCommand = caller.Context.RegisterIoCommand(new IoCommand()
        {
            Process = caller,
            Kind = IoCommandKind.httpGet,
            Location = Url,
            Command = "GET",
            Message = "reading from http stream"
        });

        try
        {
            var cancellationTokenRegistration = caller.Context.CancellationToken.Register(Client.CancelPendingRequests);

            var stream = Client.GetStreamAsync(Url).Result;
            var namedStream = new NamedStream(Url, stream, ioCommand);
            namedStream.OnDispose += (sender, args) => cancellationTokenRegistration.Dispose();
            return new[] { namedStream };
        }
        catch (Exception ex)
        {
            if (ThrowExceptionWhenFailed)
            {
                var exception = new HttpDownloadException(caller, "error while reading a http stream", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "error while reading a http stream: {0}, message: {1}",
                    Url, ex.Message));
                exception.Data["Url"] = Url;

                ioCommand.Failed(exception);
                throw exception;
            }

            ioCommand.AffectedDataCount = 0;
            ioCommand.End();
            return Enumerable.Empty<NamedStream>();
        }
    }
}