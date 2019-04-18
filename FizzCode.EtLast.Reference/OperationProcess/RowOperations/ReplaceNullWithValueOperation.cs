namespace FizzCode.EtLast
{
    /// <summary>
    /// Operation doesn't treat non-existing source column name as invalid state.
    /// </summary>
    public class ReplaceNullWithValueOperation : AbstractRowOperation
    {
        public IfDelegate If { get; set; }
        public string Column { get; set; }
        public object Value { get; set; }

        public override void Apply(IRow row)
        {
            var result = If == null || If.Invoke(row);
            if (result != true) return;

            if (row[Column] == null)
            {
                row.SetValue(Column, Value, this);
            }
        }

        public override void Prepare()
        {
            if (Column == null) throw new InvalidOperationParameterException(this, nameof(Column), Column, InvalidOperationParameterException.ValueCannotBeNullMessage);
            if (Value == null) throw new InvalidOperationParameterException(this, nameof(Value), Value, InvalidOperationParameterException.ValueCannotBeNullMessage);
        }
    }
}