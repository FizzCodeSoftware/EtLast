namespace FizzCode.EtLast
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.ComponentModel;

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class AbstractAggregationMutator : AbstractEvaluable, IMutator
    {
        public IEvaluable InputProcess { get; set; }
        public RowTestDelegate If { get; set; }

        public List<ColumnCopyConfiguration> FixColumns { get; set; }
        public Func<IRow, string> KeyGenerator { get; set; }

        protected AbstractAggregationMutator(ITopic topic, string name)
            : base(topic, name)
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