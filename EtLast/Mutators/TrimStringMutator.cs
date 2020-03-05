namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public class TrimStringMutator : AbstractMutator
    {
        public string[] Columns { get; set; }

        public TrimStringMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var columns = Columns ?? row.Values.Select(x => x.Key).ToArray();
            foreach (var column in columns)
            {
                var source = row[column];
                if (source is string str && !string.IsNullOrEmpty(str))
                {
                    var trimmed = str.Trim();
                    if (trimmed != str)
                    {
                        row.SetStagedValue(column, trimmed);
                    }
                }
            }

            row.ApplyStaging();

            yield return row;
        }
    }
}