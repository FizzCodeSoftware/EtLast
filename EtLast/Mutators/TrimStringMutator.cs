﻿namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public class TrimStringMutator : AbstractMutator
    {
        public string[] Columns { get; set; }

        public TrimStringMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
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

            row.ApplyStaging(this);

            yield return row;
        }
    }
}