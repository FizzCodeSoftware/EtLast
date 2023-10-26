using System.Net.Http;

namespace FizzCode.EtLast;

public class HttpStreamProvider : IStreamProvider
{
    /// <summary>
    /// According to MSDN, it is recommended to reuse HttpClient instances if possible.
    /// https://learn.microsoft.com/en-us/dotnet/fundamentals/networking/http/httpclient-guidelines
    /// </summary>
    public required HttpClient Client { get; init; }
    public required string Url { get; init; }

    /// <summary>
    /// Default value is true.
    /// </summary>
    public bool ThrowExceptionWhenFailed { get; init; } = true;

    public string GetTopic()
    {
        return Url;
    }

    public void Validate(IProcess caller)
    {
        if (Client == null)
            throw new ProcessParameterNullException(caller, nameof(Client));

        if (Url == null)
            throw new ProcessParameterNullException(caller, nameof(Url));
    }

    public IEnumerable<NamedStream> GetStreams(IProcess caller)
    {
        var iocUid = caller.Context.RegisterIoCommandStart(caller, IoCommandKind.httpGet, Url, null, null, "GET", null, null,
            "reading from http stream");

        try
        {
            var cancellationTokenRegistration = caller.Context.CancellationToken.Register(Client.CancelPendingRequests);

            var stream = Client.GetStreamAsync(Url).Result;
            var namedStream = new NamedStream(Url, stream, iocUid, IoCommandKind.httpGet);
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

                caller.Context.RegisterIoCommandFailed(caller, IoCommandKind.httpGet, iocUid, null, exception);
                throw exception;
            }

            caller.Context.RegisterIoCommandSuccess(caller, IoCommandKind.httpGet, iocUid, 0);
            return Enumerable.Empty<NamedStream>();
        }
    }
}
