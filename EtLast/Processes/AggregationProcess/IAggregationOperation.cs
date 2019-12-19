namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IAggregationOperation : IBaseOperation
    {
        IRow TransformGroup(string[] groupingColumns, IProcess process, List<IRow> rows);
    }
}