using System.Net.Http;

namespace FizzCode.EtLast;

public sealed class HttpDownloadToLocalFile : AbstractJob
{
    /// <summary>
    /// According to MSDN, it is recommended to reuse HttpClient instances if possible.
    /// https://learn.microsoft.com/en-us/dotnet/fundamentals/networking/http/httpclient-guidelines
    /// </summary>
    public required HttpClient Client { get; init; }
    public required string Url { get; init; }
    public required string OutputFileName { get; init; }

    /// <summary>
    /// Default false.
    /// </summary>
    public bool SkipIfTargetFileExistsButLarger { get; init; }

    public Dictionary<string, string> Headers { get; set; }

    public HttpDownloadToLocalFile(IEtlContext context)
        : base(context)
    {
    }

    public override void ValidateParameters()
    {
        if (Client == null)
            throw new ProcessParameterNullException(this, nameof(Client));

        if (string.IsNullOrEmpty(Url))
            throw new ProcessParameterNullException(this, nameof(Url));

        if (string.IsNullOrEmpty(OutputFileName))
            throw new ProcessParameterNullException(this, nameof(OutputFileName));
    }

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        var directory = Path.GetDirectoryName(OutputFileName);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            try
            {
                Directory.CreateDirectory(directory);
            }
            catch (Exception ex)
            {
                var exception = new HttpDownloadToLocalFileException(this, "http download to local file failed / directory creation failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "http download to local file failed: {0}, directory creation failed, message: {1}",
                    OutputFileName, ex.Message));
                exception.Data["FileName"] = OutputFileName;
                exception.Data["Directory"] = directory;
                throw exception;
            }
        }

        var iocUidHttpGet = 0;
        var iocUidFileWrite = 0;
        try
        {
            using (Context.CancellationToken.Register(Client.CancelPendingRequests))
            {
                iocUidHttpGet = Context.RegisterIoCommandStart(this, IoCommandKind.httpGet, Url, null, null, null, null,
                    "downloading file from {Url}",
                    Url, OutputFileName);

                iocUidFileWrite = Context.RegisterIoCommandStart(this, IoCommandKind.fileWrite, OutputFileName, null, null, null, null,
                    "writing downloaded content to file {FileName}",
                    OutputFileName);

                var message = new HttpRequestMessage(HttpMethod.Get, Url);
                if (Headers != null)
                {
                    foreach (var kvp in Headers)
                    {
                        message.Headers.Add(kvp.Key, kvp.Value);
                    }
                }

                using (var response = Client.SendAsync(message).Result)
                {
                    response.EnsureSuccessStatusCode();
                    using (var responseStream = response.Content.ReadAsStream())
                    {
                        if (!SkipIfTargetFileExistsButLarger || !File.Exists(OutputFileName))
                        {
                            using (var fileStream = new FileStream(OutputFileName, FileMode.Create))
                            {
                                responseStream.CopyTo(fileStream);
                            }
                        }
                        else
                        {
                            var existingLength = new FileInfo(OutputFileName).Length;
                            using (var ms = new MemoryStream())
                            {
                                responseStream.CopyTo(ms);
                                if (existingLength <= ms.Length)
                                {
                                    using (var fileStream = new FileStream(OutputFileName, FileMode.Create))
                                    {
                                        ms.Position = 0;
                                        ms.CopyTo(fileStream);
                                    }
                                }
                            }
                        }
                    }
                }

                var size = new FileInfo(OutputFileName).Length;
                Context.RegisterIoCommandSuccess(this, IoCommandKind.httpGet, iocUidHttpGet, size);
                Context.RegisterIoCommandSuccess(this, IoCommandKind.fileWrite, iocUidFileWrite, size);
            }
        }
        catch (Exception ex)
        {
            var exception = new HttpDownloadToLocalFileException(this, "http download to local file failed", ex);
            exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "http download to local file failed, url: {0}, file name: {1}, message: {2}",
                Url, OutputFileName, ex.Message));
            exception.Data["Url"] = Url;
            exception.Data["FileName"] = OutputFileName;

            Context.RegisterIoCommandFailed(this, IoCommandKind.httpGet, iocUidHttpGet, null, exception);
            Context.RegisterIoCommandFailed(this, IoCommandKind.fileWrite, iocUidFileWrite, null, exception);
            throw exception;
        }
    }
}
