namespace FizzCode.EtLast
{
    public abstract class AbstractContinuousAggregationOperation : IContinuousAggregationOperation
    {
        public string Name { get; set; }
        public string InstanceName { get; set; }
        public int Index { get; private set; }
        public IProcess Process { get; private set; }

        protected AbstractContinuousAggregationOperation()
        {
            Name = GetType().Name;
        }

        public abstract void TransformGroup(string[] groupingColumns, IProcess process, IRow row, IRow aggregateRow, int rowsInGroup);

        public void SetParent(IProcess process, int index)
        {
            Process = process;
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