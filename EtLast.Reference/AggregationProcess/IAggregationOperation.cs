namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IAggregationOperation : IBaseOperation
    {
        IEnumerable<IRow> TransformGroup(string[] groupingColumns, IProcess process, List<IRow> rows);
    }
}