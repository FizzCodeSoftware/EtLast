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

                using (var response = Client.GetStreamAsync(Url).Result)
                using (var fileStream = new FileStream(OutputFileName, FileMode.Create))
                {
                    response.CopyTo(fileStream);
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
