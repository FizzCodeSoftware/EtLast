namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IAggregationOperation
    {
        IProcess Process { get; }
        void SetProcess(IProcess process);
        IRow TransformGroup(string[] groupingColumns, List<IRow> rows);
    }
}