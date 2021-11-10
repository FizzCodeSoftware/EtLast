﻿namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;

    public class ReplaceStringMutator : AbstractMutator
    {
        public string ColumnName { get; init; }
        public string OldString { get; init; }
        public string NewString { get; init; }

        /// <summary>
        /// Default value is <see cref="StringComparison.InvariantCulture"/>.
        /// </summary>
        public StringComparison StringComparison { get; init; } = StringComparison.InvariantCulture;

        public ReplaceStringMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            try
            {
                if (row.HasValue(ColumnName) && row[ColumnName] is string value && value.IndexOf(OldString, StringComparison) > -1)
                {
                    value = value.Replace(OldString, NewString, StringComparison);
                    row[ColumnName] = value;
                }
            }
            catch (Exception ex) when (ex is not EtlException)
            {
                throw new ProcessExecutionException(this, row, ex);
            }

            yield return row;
        }

        protected override void ValidateMutator()
        {
            if (ColumnName == null)
                throw new ProcessParameterNullException(this, nameof(ColumnName));

            if (string.IsNullOrEmpty(OldString))
                throw new ProcessParameterNullException(this, nameof(OldString));
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class ReplaceStringMutatorFluent
    {
        public static IFluentProcessMutatorBuilder ReplaceString(this IFluentProcessMutatorBuilder builder, ReplaceStringMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}