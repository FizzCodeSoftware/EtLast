using System.Net.Http;

namespace FizzCode.EtLast;
public sealed class HttpDownload : AbstractJob
{
    /// <summary>
    /// According to MSDN, it is recommended to reuse HttpClient instances if possible.
    /// https://learn.microsoft.com/en-us/dotnet/fundamentals/networking/http/httpclient-guidelines
    /// </summary>
    [ProcessParameterMustHaveValue]
    public required HttpClient Client { get; init; }

    [ProcessParameterMustHaveValue]
    public required string Url { get; init; }

    [ProcessParameterMustHaveValue]
    public required MemoryStream OutputStream { get; init; }

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        IoCommand ioCommand = null;
        try
        {
            using (Context.CancellationToken.Register(Client.CancelPendingRequests))
            {
                ioCommand = Context.RegisterIoCommand(new IoCommand()
                {
                    Process = this,
                    Kind = IoCommandKind.httpGet,
                    Location = Url,
                    Command = "GET",
                    Message = "downloading file",
                });

                var initialSize = OutputStream.Length;
                using (var response = Client.GetStreamAsync(Url).Result)
                    response.CopyTo(OutputStream);

                var downloadedBytes = OutputStream.Length - initialSize;
                ioCommand.AffectedDataCount += downloadedBytes;
                ioCommand.End();
            }
        }
        catch (Exception ex)
        {
            var exception = new HttpDownloadException(this, "http download failed", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "http download failed, url: {0}, message: {1}",
                Url, ex.Message));
            exception.Data["Url"] = Url;

            ioCommand.Failed(exception);
            throw exception;
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class HttpDownloadFluent
{
    public static IFlow HttpDownload(this IFlow builder, Func<HttpDownload> processCreator)
    {
        return builder.ExecuteProcess(processCreator);
    }
}