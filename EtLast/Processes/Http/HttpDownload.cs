﻿using System.Net.Http;

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
        var ioCommandId = 0L;
        try
        {
            using (Context.CancellationToken.Register(Client.CancelPendingRequests))
            {
                ioCommandId = Context.RegisterIoCommandStartWithLocation(this, IoCommandKind.httpGet, Url, null, "GET", null, null,
                    "downloading file", null);

                var initialSize = OutputStream.Length;
                using (var response = Client.GetStreamAsync(Url).Result)
                    response.CopyTo(OutputStream);
                var downloadedBytes = OutputStream.Length - initialSize;

                Context.RegisterIoCommandSuccess(this, IoCommandKind.httpGet, ioCommandId, downloadedBytes);
            }
        }
        catch (Exception ex)
        {
            var exception = new HttpDownloadException(this, "http download failed", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "http download failed, url: {0}, message: {1}",
                Url, ex.Message));
            exception.Data["Url"] = Url;

            Context.RegisterIoCommandFailed(this, IoCommandKind.httpGet, ioCommandId, null, exception);
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