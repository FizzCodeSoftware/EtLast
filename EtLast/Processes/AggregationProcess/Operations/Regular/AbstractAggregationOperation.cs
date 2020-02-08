namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public abstract class AbstractAggregationOperation : IAggregationOperation
    {
        public IProcess Process { get; private set; }

        public abstract IRow TransformGroup(string[] groupingColumns, List<IRow> rows);

        public void SetProcess(IProcess process)
        {
            Process = process;
        }
    }
}