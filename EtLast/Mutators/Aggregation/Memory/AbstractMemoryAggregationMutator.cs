namespace FizzCode.EtLast
{
    public abstract class AbstractMemoryAggregationMutator : AbstractAggregationMutator
    {
        private IMemoryAggregationOperation _operation;

        protected AbstractMemoryAggregationMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        public IMemoryAggregationOperation Operation
        {
            get => _operation;
            set
            {
                _operation?.SetProcess(null);

                _operation = value;
                _operation.SetProcess(this);
            }
        }

        protected override void ValidateImpl()
        {
            if (GroupingColumns == null || GroupingColumns.Count == 0)
                throw new ProcessParameterNullException(this, nameof(GroupingColumns));

            if (Operation == null)
                throw new ProcessParameterNullException(this, nameof(Operation));
        }
    }
}