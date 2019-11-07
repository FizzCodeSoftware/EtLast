namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;

    public class DeleteFileProcess : AbstractExecutableProcess
    {
        public string FileName { get; set; }

        public DeleteFileProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        public override void Validate()
        {
            if (string.IsNullOrEmpty(FileName))
                throw new ProcessParameterNullException(this, nameof(FileName));
        }

        protected override void Execute(Stopwatch startedOn)
        {
            if (!File.Exists(FileName))
            {
                Context.Log(LogSeverity.Debug, this, "can't delete file because it doesn't exists '{FileName}'", PathHelpers.GetFriendlyPathName(FileName));
                return;
            }

            Context.Log(LogSeverity.Information, this, null, "deleting file '{FileName}'", PathHelpers.GetFriendlyPathName(FileName));

            try
            {
                File.Delete(FileName);
                Context.Log(LogSeverity.Debug, this, "successfully deleted file '{FileName}' in {Elapsed}", PathHelpers.GetFriendlyPathName(FileName),
                    startedOn.Elapsed);
            }
            catch (Exception ex)
            {
                var exception = new ProcessExecutionException(this, "file deletion failed", ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "file deletion failed, file name: {0}, message: {1}",
                    FileName, ex.Message));
                exception.Data.Add("FileName", FileName);
                throw exception;
            }
        }
    }
}