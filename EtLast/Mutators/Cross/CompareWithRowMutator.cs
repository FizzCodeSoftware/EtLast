namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;

    public class CompareWithRowMutator : AbstractCrossMutator
    {
        public RowKeyGenerator RowKeyGenerator { get; set; }
        public IRowEqualityComparer EqualityComparer { get; set; }
        public MatchAction MatchAndEqualsAction { get; set; }
        public MatchAction MatchButDifferentAction { get; set; }
        public NoMatchAction NoMatchAction { get; set; }
        private RowLookup _lookup;

        public CompareWithRowMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void StartMutator()
        {
            _lookup = LookupBuilder.Build(this);
        }

        protected override void CloseMutator()
        {
            _lookup.Clear();
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var removeRow = false;
            var key = GenerateRowKey(row);
            if (_lookup.CountByKey(key) == 0)
            {
                if (NoMatchAction != null)
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
            }
            else
            {
                var match = _lookup.GetSingleRowByKey(key);
                var isSame = EqualityComparer.Equals(row, match);
                if (isSame)
                {
                    if (MatchAndEqualsAction != null)
                    {
                        switch (MatchAndEqualsAction.Mode)
                        {
                            case MatchMode.Remove:
                                removeRow = true;
                                break;
                            case MatchMode.Throw:
                                var exception = new ProcessExecutionException(this, row, "match");
                                exception.Data.Add("Key", key);
                                throw exception;
                            case MatchMode.Custom:
                                MatchAndEqualsAction.InvokeCustomAction(this, row, match);
                                break;
                            case MatchMode.CustomThenRemove:
                                removeRow = true;
                                MatchAndEqualsAction.InvokeCustomAction(this, row, match);
                                break;
                        }
                    }
                }
                else if (MatchButDifferentAction != null)
                {
                    switch (MatchButDifferentAction.Mode)
                    {
                        case MatchMode.Remove:
                            removeRow = true;
                            break;
                        case MatchMode.Throw:
                            var exception = new ProcessExecutionException(this, row, "no match");
                            exception.Data.Add("Key", key);
                            throw exception;
                        case MatchMode.Custom:
                            MatchButDifferentAction.InvokeCustomAction(this, row, match);
                            break;
                        case MatchMode.CustomThenRemove:
                            removeRow = true;
                            MatchButDifferentAction.InvokeCustomAction(this, row, match);
                            break;
                    }
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

            if (MatchAndEqualsAction == null && NoMatchAction == null && MatchButDifferentAction == null)
                throw new InvalidProcessParameterException(this, nameof(MatchAndEqualsAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must be specified: " + nameof(MatchAndEqualsAction) + " or " + nameof(NoMatchAction) + " or " + nameof(MatchButDifferentAction));

            if (MatchAndEqualsAction?.Mode == MatchMode.Custom && MatchAndEqualsAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(MatchAndEqualsAction) + "." + nameof(MatchAndEqualsAction.CustomAction));

            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));

            if (NoMatchAction != null && MatchAndEqualsAction != null
                && ((NoMatchAction.Mode == MatchMode.Remove && MatchAndEqualsAction.Mode == MatchMode.Remove)
                    || (NoMatchAction.Mode == MatchMode.Throw && MatchAndEqualsAction.Mode == MatchMode.Throw)))
            {
                throw new InvalidProcessParameterException(this, nameof(MatchAndEqualsAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must use a different action mode: " + nameof(MatchAndEqualsAction) + " or " + nameof(NoMatchAction));
            }

            if (EqualityComparer == null)
                throw new ProcessParameterNullException(this, nameof(EqualityComparer));
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
    public static class CompareWithRowMutatorFluent
    {
        public static IFluentProcessMutatorBuilder AddCompareWithRowMutator(this IFluentProcessMutatorBuilder builder, CompareWithRowMutator mutator)
        {
            return builder.AddMutators(mutator);
        }
    }
}