namespace FizzCode.EtLast
{
    public class DebugFlagRowsOperation : AbstractRowOperation
    {
        public IfRowDelegate If { get; set; }

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            row.Flagged = true;
        }

        public override void Prepare()
        {
        }
    }
}