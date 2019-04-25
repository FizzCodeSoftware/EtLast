namespace FizzCode.EtLast
{
    public class DebugFlagRowsOperation : AbstractRowOperation
    {
        public IfDelegate If { get; set; }

        public override void Apply(IRow row)
        {
            var result = If?.Invoke(row) != false;
            if (!result) return;

            row.Flagged = true;
        }

        public override void Prepare()
        {
        }
    }
}