namespace FizzCode.EtLast
{
    public abstract class AbstractContinuousAggregationOperation : IContinuousAggregationOperation
    {
        public int UID { get; private set; }
        public string Name { get; set; }
        public string InstanceName { get; set; }
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

        public void Prepare()
        {
            UID = Process.Context.GetOperationUid(this);
        }

        public void Shutdown()
        {
        }
    }
}