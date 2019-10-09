namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Threading;

    public class DeleteFileJob : AbstractJob
    {
        public string FileName { get; set; }

        public override void Execute(IProcess process, CancellationTokenSource cancellationTokenSource)
        {
            if (string.IsNullOrEmpty(FileName))
                throw new JobParameterNullException(process, this, nameof(FileName));

            if (File.Exists(FileName))
            {
                process.Context.Log(LogSeverity.Information, process, "({Job}) deleting file '{FileName}'",
                    Name, PathHelpers.GetFriendlyPathName(FileName));

                var startedOn = Stopwatch.StartNew();
                try
                {
                    File.Delete(FileName);
                    process.Context.Log(LogSeverity.Debug, process, "({Job}) successfully deleted file '{FileName}' in {Elapsed}",
                        Name, PathHelpers.GetFriendlyPathName(FileName), startedOn.Elapsed);
                }
                catch (Exception ex)
                {
                    var exception = new JobExecutionException(process, this, "file deletion failed", ex);
                    exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "file deletion failed, file name: {0}, message: {1}",
                        FileName, ex.Message));
                    exception.Data.Add("FileName", FileName);
                    throw exception;
                }
            }
            else
            {
                process.Context.Log(LogSeverity.Debug, process, "can't delete file because it doesn't exists '{FileName}'", PathHelpers.GetFriendlyPathName(FileName));
            }
        }
    }
}