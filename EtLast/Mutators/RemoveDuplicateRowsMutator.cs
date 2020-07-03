namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Input can be unordered.
    /// - discards input rows on-the-fly
    /// - keeps already yielded row KEYS in memory (!)
    /// </summary>
    public class RemoveDuplicateRowsMutator : AbstractMutator
    {
        public Func<IReadOnlyRow, string> KeyGenerator { get; set; }

        private readonly HashSet<string> _returnedKeys = new HashSet<string>();

        public RemoveDuplicateRowsMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (KeyGenerator == null)
                throw new ProcessParameterNullException(this, nameof(KeyGenerator));
        }

        protected override void CloseMutator()
        {
            base.CloseMutator();

            _returnedKeys.Clear();
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var key = KeyGenerator.Invoke(row);
            if (!_returnedKeys.Contains(key))
            {
                _returnedKeys.Add(key);

                yield return row;
            }
            else
            {
                Context.SetRowOwner(row, null);
            }
        }
    }
}