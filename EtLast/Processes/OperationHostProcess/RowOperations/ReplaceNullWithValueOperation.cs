namespace FizzCode.EtLast
{
    /// <summary>
    /// Operation doesn't treat non-existing source column name as invalid state.
    /// </summary>
    public class ReplaceNullWithValueOperation : AbstractRowOperation
    {
        public RowTestDelegate If { get; set; }
        public string Column { get; set; }
        public object Value { get; set; }

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            if (row[Column] == null)
            {
                row.SetValue(Column, Value, this);
            }
        }

        public override void Prepare()
        {
            if (Column == null)
                throw new OperationParameterNullException(this, nameof(Column));

            if (Value == null)
                throw new OperationParameterNullException(this, nameof(Value));
        }
    }
}