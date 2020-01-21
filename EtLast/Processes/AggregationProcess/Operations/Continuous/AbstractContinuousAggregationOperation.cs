namespace FizzCode.EtLast
{
    public abstract class AbstractContinuousAggregationOperation : IContinuousAggregationOperation
    {
        public string Name { get; set; }
        public string InstanceName { get; set; }
        public int Number { get; private set; }
        public IProcess Process { get; private set; }

        protected AbstractContinuousAggregationOperation()
        {
            Name = GetType().GetFriendlyTypeName();
        }

        public abstract void TransformGroup(string[] groupingColumns, IProcess process, IRow row, IRow aggregateRow, int rowsInGroup);

        public void SetProcess(IProcess process)
        {
            Process = process;
        }

        public void SetNumber(int number)
        {
            Number = number;
        }

        public void Prepare()
        {
        }

        public void Shutdown()
        {
        }
    }
}