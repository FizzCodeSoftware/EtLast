namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class KeyTestMutator : AbstractCrossMutator
    {
        public RowKeyGenerator RowKeyGenerator { get; set; }
        public NoMatchAction NoMatchAction { get; set; }
        public MatchAction MatchAction { get; set; }

        /// <summary>
        /// Default true. Setting to false results to significantly less memory usage.
        /// </summary>
        public bool MatchActionContainsMatch { get; set; } = true;

        private ICountableLookup _lookup;

        public KeyTestMutator(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected override void StartMutator()
        {
            _lookup = MatchActionContainsMatch
                ? (ICountableLookup)new RowLookup()
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
            var matchCount = _lookup.GetRowCountByKey(key);
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
                            IRow match = null;
                            if (MatchActionContainsMatch)
                            {
                                match = (_lookup as RowLookup).GetSingleRowByKey(key);
                            }

                            MatchAction.InvokeCustomAction(this, row, match);
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

            if (NoMatchAction != null && MatchAction != null && ((NoMatchAction.Mode == MatchMode.Remove && MatchAction.Mode == MatchMode.Remove) || (NoMatchAction.Mode == MatchMode.Throw && MatchAction.Mode == MatchMode.Throw)))
                throw new InvalidProcessParameterException(this, nameof(MatchAction) + "&" + nameof(NoMatchAction), null, "at least one of these parameters must use a different action moode: " + nameof(MatchAction) + " or " + nameof(NoMatchAction));
        }

        private string GenerateRowKey(IRow row)
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
}