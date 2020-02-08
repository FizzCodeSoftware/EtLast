namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class ReplaceEmptyStringWithNullMutator : AbstractMutator
    {
        public string[] Columns { get; set; }

        public ReplaceEmptyStringWithNullMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var columns = Columns ?? row.Values.Select(kvp => kvp.Key).ToArray();
            foreach (var column in Columns)
            {
#pragma warning disable CA1820 // Test for empty strings using string length
                if (string.Equals(row.GetAs<string>(column, null), string.Empty, StringComparison.InvariantCultureIgnoreCase))
#pragma warning restore CA1820 // Test for empty strings using string length
                {
                    row.SetValue(column, null, this);
                }
            }

            yield return row;
        }
    }
}