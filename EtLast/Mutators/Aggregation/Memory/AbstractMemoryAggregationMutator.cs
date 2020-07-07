namespace FizzCode.EtLast
{
    using System.ComponentModel;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
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
            if (KeyGenerator == null)
                throw new ProcessParameterNullException(this, nameof(KeyGenerator));

            if (Operation == null)
                throw new ProcessParameterNullException(this, nameof(Operation));
        }
    }
}