namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Linq;

    public class UnionMerger : IRowSetMerger
    {
        public IEnumerable<IRow> Merge(List<IEnumerable<IRow>> input)
        {
            if (input.Count == 0)
            {
                return Enumerable.Empty<IRow>();
            }

            var x = input[0];
            for (int i = 1; i < input.Count; i++)
            {
                x = x.Union(input[i]);
            }

            return x;
        }
    }
}