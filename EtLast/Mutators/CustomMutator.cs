namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public delegate bool CustomMutatorDelegate(IProcess process, IRow row);

    public class CustomMutator : AbstractMutator
    {
        public CustomMutatorDelegate Then { get; set; }

        public CustomMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var keep = true;
            try
            {
                keep = Then.Invoke(this, row);
            }
            catch (Exception ex) when (!(ex is EtlException))
            {
                throw new ProcessExecutionException(this, row, ex);
            }

            if (keep)
                yield return row;
        }

        protected override void ValidateMutator()
        {
            if (Then == null)
                throw new ProcessParameterNullException(this, nameof(Then));
        }
    }
}