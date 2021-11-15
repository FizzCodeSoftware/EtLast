﻿namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;

    public delegate bool CustomMutatorDelegate(IRow row);

    public sealed class CustomMutator : AbstractMutator
    {
        public CustomMutatorDelegate Then { get; init; }

        public CustomMutator(IEtlContext context, string topic, string name)
            : base(context, topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var tracker = new TrackedRow(row);
            bool keep;
            try
            {
                keep = Then.Invoke(tracker);
                if (keep)
                {
                    tracker.ApplyChanges();
                }
            }
            catch (Exception ex)
            {
                var exception = new CustomCodeException(this, "error during the execution of custom code", ex);
                throw exception;
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