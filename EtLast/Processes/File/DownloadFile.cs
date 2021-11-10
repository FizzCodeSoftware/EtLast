namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Net.Http;

    public class DownloadFile : AbstractExecutable
    {
        public string Url { get; set; }
        public string FileName { get; set; }

        public DownloadFile(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            if (string.IsNullOrEmpty(Url))
                throw new ProcessParameterNullException(this, nameof(Url));

            if (string.IsNullOrEmpty(FileName))
                throw new ProcessParameterNullException(this, nameof(FileName));
        }

        protected override void ExecuteImpl()
        {
            using (var clt = new HttpClient())
            {
                var iocUid = 0;
                try
                {
                    using (Context.CancellationTokenSource.Token.Register(clt.CancelPendingRequests))
                    {
                        iocUid = Context.RegisterIoCommandStart(this, IoCommandKind.httpGet, Url, null, null, null, null,
                            "downloading file from {Url} to {FileName}",
                            Url, PathHelpers.GetFriendlyPathName(FileName));

                        using (var response = clt.GetStreamAsync(Url).Result)
                        using (var fs = new FileStream(FileName, FileMode.Create))
                        {
                            response.CopyTo(fs);
                        }

                        Context.RegisterIoCommandSuccess(this, IoCommandKind.httpGet, iocUid, Convert.ToInt32(new FileInfo(FileName).Length));
                    }
                }
                catch (Exception ex)
                {
                    Context.RegisterIoCommandFailed(this, IoCommandKind.httpGet, iocUid, null, ex);

                    var exception = new ProcessExecutionException(this, "file download failed", ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "file download failed, url: {0}, file name: {1}, message: {2}",
                        Url, FileName, ex.Message));
                    exception.Data.Add("Url", Url);
                    exception.Data.Add("FileName", FileName);
                    throw exception;
                }
            }
        }
    }
}