namespace FizzCode.EtLast
{
    public class RemoveRowOperation : AbstractRowOperation
    {
        public IfDelegate If { get; set; }

        public override void Apply(IRow row)
        {
            var result = If?.Invoke(row) != false;
            if (!result) return;

            Process.RemoveRow(row, this);
        }

        public override void Prepare()
        {
        }
    }
}