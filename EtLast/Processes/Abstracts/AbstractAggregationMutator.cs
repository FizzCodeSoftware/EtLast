namespace FizzCode.EtLast
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.ComponentModel;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class AbstractAggregationMutator : AbstractEvaluable, IMutator
    {
        public IProducer InputProcess { get; set; }
        public RowTestDelegate If { get; set; }
        public RowTagTestDelegate TagFilter { get; set; }

        public List<ColumnCopyConfiguration> FixColumns { get; init; }
        public Func<IRow, string> KeyGenerator { get; init; }

        protected AbstractAggregationMutator(IEtlContext context)
            : base(context)
        {
        }

        public IEnumerator<IMutator> GetEnumerator()
        {
            yield return this;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            yield return this;
        }
    }
}