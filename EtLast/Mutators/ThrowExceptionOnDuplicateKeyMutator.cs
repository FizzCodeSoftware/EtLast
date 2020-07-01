namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class ThrowExceptionOnDuplicateKeyMutator : AbstractMutator
    {
        public Func<IReadOnlyRow, string> RowKeyGenerator { get; set; }

        private readonly HashSet<string> _keys = new HashSet<string>();

        public ThrowExceptionOnDuplicateKeyMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (RowKeyGenerator == null)
                throw new ProcessParameterNullException(this, nameof(RowKeyGenerator));
        }

        protected override void CloseMutator()
        {
            base.CloseMutator();

            _keys.Clear();
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var key = RowKeyGenerator.Invoke(row);
            if (_keys.Contains(key))
            {
                var exception = new EtlException(this, "duplicate key found");
                exception.Data.Add("Row", row.ToDebugString());
                exception.Data.Add("Key", key);

                throw exception;
            }

            _keys.Add(key);

            yield return row;
        }
    }
}