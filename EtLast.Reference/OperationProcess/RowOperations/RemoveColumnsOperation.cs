namespace FizzCode.EtLast
{
    /// <summary>
    /// Operation doesn't treat non-existing source columns as invalid state.
    /// </summary>
    public class RemoveColumnsOperation : AbstractRowOperation
    {
        public IfDelegate If { get; set; }
        public string[] Columns { get; set; }

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            foreach (var column in Columns)
            {
                row.RemoveColumn(column, this);
            }
        }

        public override void Prepare()
        {
            if (Columns == null || Columns.Length == 0)
                throw new OperationParameterNullException(this, nameof(Columns));
        }
    }
}