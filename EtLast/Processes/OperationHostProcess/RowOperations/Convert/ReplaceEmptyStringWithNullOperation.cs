namespace FizzCode.EtLast
{
    public class ReplaceEmptyStringWithNullOperation : AbstractRowOperation
    {
        public RowTestDelegate If { get; set; }
        public string[] Columns { get; set; }

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            if (Columns != null)
            {
                foreach (var column in Columns)
                {
                    var source = row[column];
                    if (source is string str && string.IsNullOrEmpty(str))
                    {
                        row.SetValue(column, null, this);
                    }
                }
            }
            else
            {
                foreach (var kvp in row.Values)
                {
                    var source = row[kvp.Key];
                    if (source is string str && string.IsNullOrEmpty(str))
                    {
                        row.SetValue(kvp.Key, null, this);
                    }
                }
            }
        }

        public override void Prepare()
        {
        }
    }
}