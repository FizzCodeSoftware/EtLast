namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public abstract class AbstractAggregationOperation : IAggregationOperation
    {
        public string Name { get; private set; }

        private string _instanceName;

        public string InstanceName
        {
            get => _instanceName;
            set
            {
                _instanceName = value;
                Name = value;
            }
        }

        public int Number { get; private set; }
        public IProcess Process { get; private set; }

        protected AbstractAggregationOperation()
        {
            Name = GetType().GetFriendlyTypeName();
        }

        public abstract IRow TransformGroup(string[] groupingColumns, IProcess process, List<IRow> rows);

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