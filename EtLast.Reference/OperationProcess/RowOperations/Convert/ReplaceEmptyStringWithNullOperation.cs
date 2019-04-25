namespace FizzCode.EtLast
{
    public class ReplaceEmptyStringWithNullOperation : AbstractRowOperation
    {
        public IfDelegate If { get; set; }
        public string[] Columns { get; set; }

        public override void Apply(IRow row)
        {
            var result = If?.Invoke(row) != false;
            if (!result) return;

            if (Columns != null)
            {
                foreach (var column in Columns)
                {
                    var source = row[column];
                    if (source is string str && str == string.Empty)
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
                    if (source is string str && str == string.Empty)
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