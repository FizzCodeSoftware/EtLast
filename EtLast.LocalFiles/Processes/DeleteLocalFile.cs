namespace FizzCode.EtLast
{
    using System;
    using System.Globalization;
    using System.IO;

    public sealed class DeleteLocalFile : AbstractExecutable
    {
        public string FileName { get; init; }

        public DeleteLocalFile(IEtlContext context)
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
                Context.Log(LogSeverity.Debug, this, "can't delete local file because it doesn't exist '{FileName}'", PathHelpers.GetFriendlyPathName(FileName));
                return;
            }

            Context.Log(LogSeverity.Information, this, "deleting local file '{FileName}'", PathHelpers.GetFriendlyPathName(FileName));

            try
            {
                File.Delete(FileName);
                Context.Log(LogSeverity.Debug, this, "successfully deleted local file '{FileName}' in {Elapsed}", PathHelpers.GetFriendlyPathName(FileName),
                    InvocationInfo.LastInvocationStarted.Elapsed);
            }
            catch (Exception ex)
            {
                var exception = new LocalFileDeleteException(this, "local file deletion failed", FileName, ex);
                exception.AddOpsMessage(string.Format(CultureInfo.InvariantCulture, "local file deletion failed, file name: {0}, message: {1}",
                    FileName, ex.Message));
                exception.Data.Add("FileName", FileName);
                throw exception;
            }
        }
    }
}