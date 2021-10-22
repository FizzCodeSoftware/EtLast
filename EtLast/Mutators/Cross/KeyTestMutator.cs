namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;

    public class KeyTestMutator : AbstractCrossMutator
    {
        public RowKeyGenerator RowKeyGenerator { get; init; }
        public NoMatchAction NoMatchAction { get; init; }
        public MatchAction MatchAction { get; init; }

        /// <summary>
        /// Default true. If <see cref="MatchAction.CustomAction"/> is used then setting this to false results in significantly less memory usage.
        /// </summary>
        public bool MatchActionContainsMatch { get; init; } = true;

        private ICountableLookup _lookup;

        public KeyTestMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void StartMutator()
        {
            _lookup = MatchActionContainsMatch && MatchAction?.CustomAction != null
                ? new RowLookup()
                : new CountableOnlyRowLookup();

            LookupBuilder.Append(_lookup, this);
        }

        protected override void CloseMutator()
        {
            _lookup.Clear();
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var key = GenerateRowKey(row);
            var matchCount = _lookup.CountByKey(key);
            var removeRow = false;
            if (matchCount > 0)
            {
                if (MatchAction != null)
                {
                    switch (MatchAction.Mode)
                    {
                        case MatchMode.Remove:
                            removeRow = true;
                            break;
                        case MatchMode.Throw:
                            var exception2 = new ProcessExecutionException(this, row, "match");
                            exception2.Data.Add("Key", key);
                            throw exception2;
                        case MatchMode.Custom:
                            {
                                IReadOnlySlimRow match = null;
                                if (MatchActionContainsMatch)
                                {
                                    match = (_lookup as RowLookup).GetSingleRowByKey(key);
                                }

                                MatchAction.InvokeCustomAction(this, row, match);
                            }
                            break;
                        case MatchMode.CustomThenRemove:
                            {
                                removeRow = true;

                                IReadOnlySlimRow match = null;
                                if (MatchActionContainsMatch)
                                {
                                    match = (_lookup as RowLookup).GetSingleRowByKey(key);
                                }

                                MatchAction.InvokeCustomAction(this, row, match);
                            }
                            break;
                    }
                }
            }
            else if (NoMatchAction != null)
            {
                switch (NoMatchAction.Mode)
                {
                    case MatchMode.Remove:
                        removeRow = true;
                        break;
                    case MatchMode.Throw:
                        var exception = new ProcessExecutionException(this, row, "no match");
                        exception.Data.Add("Key", key);
                        throw exception;
                    case MatchMode.Custom:
                        NoMatchAction.InvokeCustomAction(this, row);
                        break;
                    case MatchMode.CustomThenRemove:
                        removeRow = true;
                        NoMatchAction.InvokeCustomAction(this, row);
                        break;
                }
            }

            if (!removeRow)
                yield return row;
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (RowKeyGenerator == null)
                throw new ProcessParameterNullException(this, nameof(RowKeyGenerator));

            if (MatchAction == null && NoMatchAction == null)
                throw new InvalidProcessParameterException(this, nameof(MatchAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must be specified: " + nameof(MatchAction) + " or " + nameof(NoMatchAction));

            if (MatchAction?.Mode == MatchMode.Custom && MatchAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(MatchAction) + "." + nameof(MatchAction.CustomAction));

            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));

            if (NoMatchAction != null && MatchAction != null
                && ((NoMatchAction.Mode == MatchMode.Remove && MatchAction.Mode == MatchMode.Remove)
                    || (NoMatchAction.Mode == MatchMode.Throw && MatchAction.Mode == MatchMode.Throw)))
            {
                throw new InvalidProcessParameterException(this, nameof(MatchAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must use a different action moode: " + nameof(MatchAction) + " or " + nameof(NoMatchAction));
            }
        }

        private string GenerateRowKey(IReadOnlyRow row)
        {
            try
            {
                return RowKeyGenerator(row);
            }
            catch (EtlException) { throw; }
            catch (Exception)
            {
                var exception = new ProcessExecutionException(this, row, nameof(RowKeyGenerator) + " failed");
                throw exception;
            }
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class KeyTestMutatorFluent
    {
        /// <summary>
        /// Tests row keys and execute <see cref="NoMatchAction"/> or <see cref="MatchAction"/> based on the result of each row.
        /// - the existing rows are read from a single <see cref="RowLookup"/>
        /// - if MatchAction.CustomAction is not null and MatchActionContainsMatch is true then all rows of the lookup are kept in the memory, otherwise a <see cref="CountableOnlyRowLookup"/> is used.
        /// </summary>
        public static IFluentProcessMutatorBuilder KeyTest(this IFluentProcessMutatorBuilder builder, KeyTestMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}