namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;

    public delegate IEnumerable<IReadOnlySlimRow> EnumerableImporterDelegate(EnumerableImporter process);

    public sealed class EnumerableImporter : AbstractRowSource
    {
        public EnumerableImporterDelegate InputGenerator { get; set; }
        public Dictionary<string, ReaderColumnConfiguration> ColumnConfiguration { get; set; }
        public ReaderDefaultColumnConfiguration DefaultColumnConfiguration { get; set; }

        /// <summary>
        /// Default false.
        /// </summary>
        public bool CopyOnlySpecifiedColumns { get; set; }

        public EnumerableImporter(IEtlContext context, string topic, string name)
            : base(context, topic, name)
        {
        }

        protected override void ValidateImpl()
        {
            if (InputGenerator == null)
                throw new ProcessParameterNullException(this, nameof(InputGenerator));
        }

        protected override IEnumerable<IRow> Produce()
        {
            var inputRows = InputGenerator.Invoke(this);

            if (ColumnConfiguration != null)
            {
                var initialValues = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

                if (CopyOnlySpecifiedColumns)
                {
                    foreach (var row in inputRows)
                    {
                        if (Context.CancellationTokenSource.IsCancellationRequested)
                            yield break;

                        foreach (var kvp in ColumnConfiguration)
                        {
                            var value = HandleConverter(row[kvp.Key], kvp.Value);
                            initialValues[kvp.Value.RowColumn ?? kvp.Key] = value;
                        }

                        var newRow = Context.CreateRow(this, initialValues);
                        newRow.Tag = row.Tag;

                        yield return newRow;
                        initialValues.Clear();
                    }
                }
                else
                {
                    var columnMap = ColumnConfiguration != null
                        ? new Dictionary<string, ReaderColumnConfiguration>(ColumnConfiguration, StringComparer.InvariantCultureIgnoreCase)
                        : null;

                    foreach (var row in inputRows)
                    {
                        if (Context.CancellationTokenSource.IsCancellationRequested)
                            yield break;

                        foreach (var kvp in ColumnConfiguration)
                        {
                            var value = HandleConverter(row[kvp.Key], kvp.Value);
                            initialValues[kvp.Value.RowColumn ?? kvp.Key] = value;
                        }

                        foreach (var kvp in row.Values)
                        {
                            if (!columnMap.ContainsKey(kvp.Key.ToUpperInvariant()))
                            {
                                if (DefaultColumnConfiguration != null)
                                {
                                    var value = HandleConverter(kvp.Value, DefaultColumnConfiguration);
                                    initialValues[kvp.Key] = value;
                                }
                                else
                                {
                                    initialValues[kvp.Key] = kvp.Value;
                                }
                            }
                        }

                        var newRow = Context.CreateRow(this, initialValues);
                        newRow.Tag = row.Tag;

                        yield return newRow;
                        initialValues.Clear();
                    }
                }
            }
            else
            {
                foreach (var row in inputRows)
                {
                    if (Context.CancellationTokenSource.IsCancellationRequested)
                        yield break;

                    yield return Context.CreateRow(this, row);
                }
            }
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class EnumerableImporterFluent
    {
        public static IFluentProcessMutatorBuilder ImportEnumerable(this IFluentProcessBuilder builder, EnumerableImporter importer)
        {
            return builder.ReadFrom(importer);
        }
    }
}