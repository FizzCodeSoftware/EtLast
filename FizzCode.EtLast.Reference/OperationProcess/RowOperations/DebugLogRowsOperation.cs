namespace FizzCode.EtLast
{
    public class DebugLogRowsOperation : AbstractRowOperation
    {
        public IfDelegate If { get; set; }
        public LogSeverity Severity { get; set; } = LogSeverity.Verbose;

        public override void Apply(IRow row)
        {
            var result = If == null || If.Invoke(row);
            if (result != true) return;

            if (PrevOperation != null)
            {
                Process.Context.LogRow(Process, row, "by {PreviousOperation} ", PrevOperation.Name);
            }
            else
            {
                Process.Context.LogRow(Process, row, null);
            }
        }

        public override void Prepare()
        {
        }
    }
}