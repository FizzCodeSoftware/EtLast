using System.Linq;

namespace FizzCode.EtLast
{
    public class TrimStringOperation : AbstractRowOperation
    {
        public IfDelegate If { get; set; }
        public string[] Columns { get; set; }

        public override void Apply(IRow row)
        {
            var result = If?.Invoke(row) != false;
            if (!result) return;

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