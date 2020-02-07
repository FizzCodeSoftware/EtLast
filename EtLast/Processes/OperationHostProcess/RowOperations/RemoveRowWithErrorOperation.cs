namespace FizzCode.EtLast
{
    public class RemoveRowWithErrorOperation : AbstractRowOperation
    {
        public RowTestDelegate If { get; set; }

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            if (row.HasError())
            {
                Process.RemoveRow(row, this);
            }
        }

        protected override void PrepareImpl()
        {
        }
    }
}