﻿using System.Net.Http;

namespace FizzCode.EtLast;
public sealed class HttpDownloadToLocalFile : AbstractJob
{
    public string Url { get; init; }
    public string FileName { get; init; }

    public HttpDownloadToLocalFile(IEtlContext context)
        : base(context)
    {
    }

    public override void ValidateParameters()
    {
        if (string.IsNullOrEmpty(Url))
            throw new ProcessParameterNullException(this, nameof(Url));

        if (string.IsNullOrEmpty(FileName))
            throw new ProcessParameterNullException(this, nameof(FileName));
    }

    protected override void ExecuteImpl(Stopwatch netTimeStopwatch)
    {
        using (var clt = new HttpClient())
        {
            var iocUid = 0;
            try
            {
                using (Context.CancellationToken.Register(clt.CancelPendingRequests))
                {
                    iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.httpGet, Url, null, null, null, null,
                        "downloading file from {Url} to local file {FileName}",
                        Url, FileName);

                    using (var response = clt.GetStreamAsync(Url).Result)
                    using (var fileStream = new FileStream(FileName, FileMode.Create))
                    {
                        response.CopyTo(fileStream);
                    }

                    Context.RegisterIoCommandSuccess(this, IoCommandKind.httpGet, iocUid, Convert.ToInt32(new FileInfo(FileName).Length));
                }
            }
            catch (Exception ex)
            {
                var exception = new HttpDownloadToLocalFileException(this, "http download to local file failed", Url, FileName, ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "http download to local file failed, url: {0}, file name: {1}, message: {2}",
                    Url, FileName, ex.Message));
                exception.Data["Url"] = Url;
                exception.Data["FileName"] = FileName;

                Context.RegisterIoCommandFailed(this, IoCommandKind.httpGet, iocUid, null, exception);
                throw exception;
            }
        }
    }
}
