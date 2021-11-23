namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;
    using System.IO;

    public sealed class DeleteFile : AbstractExecutable
    {
        public string FileName { get; set; }

        public DeleteFile(IEtlContext context)
            : base(context)
        {
        }

        protected override void ValidateImpl()
        {
            if (string.IsNullOrEmpty(FileName))
                throw new ProcessParameterNullException(this, nameof(FileName));
        }

        protected override void ExecuteImpl()
        {
            if (!File.Exists(FileName))
            {
                Context.Log(LogSeverity.Debug, this, "can't delete file because it doesn't exist '{FileName}'", PathHelpers.GetFriendlyPathName(FileName));
                return;
            }

            Context.Log(LogSeverity.Information, this, "deleting file '{FileName}'", PathHelpers.GetFriendlyPathName(FileName));

            try
            {
                File.Delete(FileName);
                Context.Log(LogSeverity.Debug, this, "successfully deleted file '{FileName}' in {Elapsed}", PathHelpers.GetFriendlyPathName(FileName),
                    InvocationInfo.LastInvocationStarted.Elapsed);
            }
            catch (Exception ex)
            {
                var exception = new FileReadException(this, "file deletion failed", FileName, ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "file deletion failed, file name: {0}, message: {1}",
                    FileName, ex.Message));
                exception.Data.Add("FileName", FileName);
                throw exception;
            }
        }
    }
}