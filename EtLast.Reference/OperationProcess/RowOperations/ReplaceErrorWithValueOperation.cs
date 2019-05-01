namespace FizzCode.EtLast
{
    using System.Linq;

    /// <summary>
    /// Replaces values in <see cref="Columns"/> with <see cref="Value"/> if the value is <see cref="EtlRowError"/>.
    /// </summary>
    public class ReplaceErrorWithValueOperation : AbstractRowOperation
    {
        public IfDelegate If { get; set; }
        public string[] Columns { get; set; }
        public object Value { get; set; }

        public override void Apply(IRow row)
        {
            var result = If?.Invoke(row) != false;
            if (!result) return;

            string[] columns = Columns;

            if (columns == null)
                columns = row.Values.Select(kvp => kvp.Key).ToArray();

            foreach (var column in Columns)
            {
                if(row[column] is EtlRowError)
                    row.SetValue(column, Value, this);
            }
        }

        public override void Prepare()
        {
            if (Columns == null || Columns.Length == 0) throw new OperationParameterNullException(this, nameof(Columns));
        }
    }
}