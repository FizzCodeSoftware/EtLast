namespace FizzCode.EtLast
{
    public class TrimStringOperation : AbstractRowOperation
    {
        public IfDelegate If { get; set; }
        public string[] Columns { get; set; }

        public override void Apply(IRow row)
        {
            var result = If == null || If.Invoke(row);
            if (result != true) return;

            if (Columns != null)
            {
                foreach (var column in Columns)
                {
                    var source = row[column];
                    if (source is string str && !string.IsNullOrEmpty(str))
                    {
                        row.SetValue(column, str.Trim(), this);
                    }
                }
            }
            else
            {
                foreach (var kvp in row.Values)
                {
                    var source = row[kvp.Key];
                    if (source is string str && !string.IsNullOrEmpty(str))
                    {
                        row.SetValue(kvp.Key, str.Trim(), this);
                    }
                }
            }
        }

        public override void Prepare()
        {
        }
    }
}