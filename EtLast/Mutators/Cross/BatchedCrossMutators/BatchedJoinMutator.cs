namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class BatchedJoinMutator : AbstractBatchedCrossMutator
    {
        public List<ColumnCopyConfiguration> ColumnConfiguration { get; set; }
        public NoMatchAction NoMatchAction { get; set; }
        public MatchActionDelegate MatchCustomAction { get; set; }
        public Func<IRow, bool> MatchFilter { get; set; }

        /// <summary>
        /// Default null. If value is set, and <see cref="TooManyMatchAction"/> is null,
        /// then the excess rows will be removed, otherwise the action will be invoked.
        /// </summary>
        public int? MatchCountLimit { get; set; }

        /// <summary>
        /// Executed if the match count for a row exceeds <see cref="MatchCountLimit"/>.
        /// </summary>
        public TooManyMatchAction TooManyMatchAction { get; set; }

        /// <summary>
        /// The amount of rows processed in a batch. Default value is 1000.
        /// </summary>
        public override int BatchSize { get; set; } = 1000;

        public BatchedJoinMutator(ITopic topic, string name)
            : base(topic, name)
        {
            UseBatchKeys = true;
        }

        protected override string GetBatchKey(IRow row)
        {
            return GeneratorRowKey(row);
        }

        protected override void MutateSingleRow(IRow row, List<IRow> mutatedRows, out bool removeOriginal, out bool processed)
        {
            removeOriginal = false;
            processed = false;
        }

        protected override void MutateBatch(List<IRow> rows, List<IRow> mutatedRows, List<IRow> removedRows)
        {
            var lookup = LookupBuilder.Build(this, rows.ToArray());
            foreach (var row in rows)
            {
                var key = GeneratorRowKey(row);
                var removeRow = false;
                var matches = lookup.GetManyByKey(key, MatchFilter);
                if (MatchCountLimit != null && matches?.Count > MatchCountLimit.Value)
                {
                    if (TooManyMatchAction != null)
                    {
                        switch (TooManyMatchAction.Mode)
                        {
                            case MatchMode.Remove:
                                removeRow = true;
                                break;
                            case MatchMode.Throw:
                                var exception = new ProcessExecutionException(this, row, "too many match");
                                exception.Data.Add("Key", key);
                                throw exception;
                            case MatchMode.Custom:
                                TooManyMatchAction.CustomAction.Invoke(this, row, matches);
                                break;
                        }
                    }
                    else
                    {
                        matches.RemoveRange(MatchCountLimit.Value, matches.Count - MatchCountLimit.Value);
                    }
                }

                if (!removeRow && matches?.Count > 0)
                {
                    removeRow = true;
                    foreach (var match in matches)
                    {
                        var initialValues = new Dictionary<string, object>(row.Values);
                        ColumnCopyConfiguration.CopyMany(match, initialValues, ColumnConfiguration);

                        var newRow = Context.CreateRow(this, initialValues);

                        MatchCustomAction?.Invoke(this, newRow, match);
                        mutatedRows.Add(newRow);
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
                            NoMatchAction.CustomAction.Invoke(this, row);
                            break;
                    }
                }

                if (removeRow)
                    removedRows.Add(row);
                else
                    mutatedRows.Add(row);
            }

            lookup.Clear();
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));

            if (NoMatchAction?.Mode == MatchMode.Custom && NoMatchAction.CustomAction == null)
                throw new ProcessParameterNullException(this, nameof(NoMatchAction) + "." + nameof(NoMatchAction.CustomAction));
        }
    }
}