namespace FizzCode.EtLast
{
    using System;
    using System.Net;
    using System.Threading;

    public class DownloadFileJob : AbstractJob
    {
        public string Url { get; set; }
        public string FileName { get; set; }

        public override void Execute(IProcess process, CancellationTokenSource cancellationTokenSource)
        {
            if (string.IsNullOrEmpty(Url)) throw new InvalidJobParameterException(process, this, nameof(Url), Url, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (string.IsNullOrEmpty(FileName)) throw new InvalidJobParameterException(process, this, nameof(FileName), FileName, InvalidOperationParameterException.ValueCannotBeNullMessage);

            process.Context.Log(LogSeverity.Information, process, "downloading file from '{Url}' to '{FileName}'", Url, FileName);

            // todo: use HttpClient instead with cancellationTokenSource
            using (var clt = new WebClient())
            {
                try
                {
                    clt.DownloadFile(Url, FileName);
                }
                catch (Exception ex)
                {
                    var exception = new JobExecutionException(process, this, "file download failed", ex);
                    exception.AddOpsMessage(string.Format("file download failed, url: {0}, file name: {1}, message: {2}", Url, FileName, ex.Message));
                    exception.Data.Add("Url", Url);
                    exception.Data.Add("FileName", FileName);
                    throw exception;
                }
            }
        }
    }
}