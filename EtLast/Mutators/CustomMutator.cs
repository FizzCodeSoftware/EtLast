﻿namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;

    public delegate bool CustomMutatorDelegate(IRow row);

    public sealed class CustomMutator : AbstractMutator
    {
        public CustomMutatorDelegate Then { get; init; }

        public CustomMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var keep = true;
            try
            {
                var tracker = new TrackedRow(row);
                keep = Then.Invoke(tracker);
                if (keep)
                {
                    tracker.ApplyChanges();
                }
            }
            catch (Exception ex) when (ex is not EtlException)
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

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class CustomMutatorFluent
    {
        public static IFluentProcessMutatorBuilder CustomCode(this IFluentProcessMutatorBuilder builder, CustomMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}