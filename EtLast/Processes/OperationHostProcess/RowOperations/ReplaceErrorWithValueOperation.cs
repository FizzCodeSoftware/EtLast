namespace FizzCode.EtLast
{
    using System.Linq;

    /// <summary>
    /// Replaces values in <see cref="Columns"/> with <see cref="Value"/> if the value is <see cref="EtlRowError"/>.
    /// </summary>
    public class ReplaceErrorWithValueOperation : AbstractRowOperation
    {
        public RowTestDelegate If { get; set; }
        public string[] Columns { get; set; }
        public object Value { get; set; }

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            var columns = Columns ?? row.Values.Select(kvp => kvp.Key).ToArray();

            foreach (var column in Columns)
            {
                if (row[column] is EtlRowError)
                    row.SetValue(column, Value, this);
            }
        }

        protected override void PrepareImpl()
        {
            if (Columns == null || Columns.Length == 0)
                throw new OperationParameterNullException(this, nameof(Columns));
        }
    }
}