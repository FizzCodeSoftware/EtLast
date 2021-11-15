namespace FizzCode.EtLast
{
    using System.ComponentModel;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class AbstractMemoryAggregationMutator : AbstractAggregationMutator
    {
        private IMemoryAggregationOperation _operation;

        public IMemoryAggregationOperation Operation
        {
            get => _operation;
            init
            {
                //_operation?.SetProcess(null);

                _operation = value;
                _operation.SetProcess(this);
            }
        }

        protected AbstractMemoryAggregationMutator(IEtlContext context, string topic, string name)
            : base(context, topic, name)
        {
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