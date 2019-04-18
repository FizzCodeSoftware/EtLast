namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IRowSetMerger
    {
        IEnumerable<IRow> Merge(List<IEnumerable<IRow>> input);
    }
}