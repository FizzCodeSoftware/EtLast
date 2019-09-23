namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public abstract class AbstractAggregationOperation : IAggregationOperation
    {
        public string Name { get; set; }
        public string InstanceName { get; set; } // todo: update name when InstanceName is set
        public int Index { get; private set; }
        public IProcess Process { get; private set; }

        protected AbstractAggregationOperation()
        {
            Name = TypeHelpers.GetFriendlyTypeName(GetType());
        }

        public abstract IEnumerable<IRow> TransformGroup(string[] groupingColumns, IProcess process, List<IRow> rows);

        public void SetProcess(IProcess process)
        {
            Process = process;
        }

        public void SetParent(int index)
        {
            Index = index;
        }

        public void Prepare()
        {
        }

        public void Shutdown()
        {
        }
    }
}