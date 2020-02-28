namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IMemoryAggregationOperation
    {
        IProcess Process { get; }
        void SetProcess(IProcess process);
        IRow TransformGroup(string[] groupingColumns, List<IRow> rows);
    }
}