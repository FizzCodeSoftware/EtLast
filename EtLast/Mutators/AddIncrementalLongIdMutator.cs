namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.ComponentModel;

    public sealed class AddIncrementalLongIdMutator : AbstractMutator
    {
        public string Column { get; init; }

        /// <summary>
        /// Default value is 0.
        /// </summary>
        public long FirstId { get; init; }

        private long _nextId;

        public AddIncrementalLongIdMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void StartMutator()
        {
            _nextId = FirstId;
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            row[Column] = _nextId;
            _nextId++;
            yield return row;
        }

        protected override void ValidateMutator()
        {
            if (string.IsNullOrEmpty(Column))
                throw new ProcessParameterNullException(this, nameof(Column));
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class AddIncrementalLongIdMutatorFluent
    {
        public static IFluentProcessMutatorBuilder AddIncrementalLongId(this IFluentProcessMutatorBuilder builder, AddIncrementalLongIdMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}