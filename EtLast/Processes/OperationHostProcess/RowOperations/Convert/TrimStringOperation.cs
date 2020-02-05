namespace FizzCode.EtLast
{
    using System.Linq;

    public class TrimStringOperation : AbstractRowOperation
    {
        public RowTestDelegate If { get; set; }
        public string[] Columns { get; set; }

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            var columns = Columns ?? row.Values.Select(x => x.Key).ToArray();
            foreach (var column in columns)
            {
                var source = row[column];
                if (source is string str && !string.IsNullOrEmpty(str))
                {
                    var trimmed = str.Trim();
                    if (trimmed != str)
                    {
                        row.SetValue(column, trimmed, this);
                    }
                }
            }
        }

        public override void Prepare()
        {
        }
    }
}