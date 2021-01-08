namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Linq;

    public delegate IEnumerable<IReadOnlySlimRow> EnumerableImporterDelegate(EnumerableImporter process);

    public class EnumerableImporter : AbstractProducer
    {
        public EnumerableImporterDelegate InputGenerator { get; set; }
        public List<ReaderColumnConfiguration> ColumnConfiguration { get; set; }
        public ReaderDefaultColumnConfiguration DefaultColumnConfiguration { get; set; }

        /// <summary>
        /// Default false.
        /// </summary>
        public bool CopyOnlySpecifiedColumns { get; set; }

        public EnumerableImporter(ITopic topic, string name)
            : base(topic, name)
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

                        foreach (var config in ColumnConfiguration)
                        {
                            var value = HandleConverter(row[config.SourceColumn], config);
                            initialValues[config.RowColumn ?? config.SourceColumn] = value;
                        }

                        yield return Context.CreateRow(this, initialValues);
                        initialValues.Clear();
                    }
                }
                else
                {
                    var columnConfig = ColumnConfiguration.ToDictionary(x => x.SourceColumn.ToUpperInvariant());
                    foreach (var row in inputRows)
                    {
                        if (Context.CancellationTokenSource.IsCancellationRequested)
                            yield break;

                        foreach (var config in ColumnConfiguration)
                        {
                            var value = HandleConverter(row[config.SourceColumn], config);
                            initialValues[config.RowColumn ?? config.SourceColumn] = value;
                        }

                        foreach (var kvp in row.Values)
                        {
                            if (!columnConfig.ContainsKey(kvp.Key.ToUpperInvariant()))
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

                        yield return Context.CreateRow(this, initialValues);
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

                    yield return Context.CreateRow(this, row.Values);
                }
            }
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class EnumerableImporterFluent
    {
        public static IFluentProcessMutatorBuilder ImportEnumerable(this IFluentProcessBuilder builder, EnumerableImporter producer)
        {
            return builder.ReadFrom(producer);
        }
    }
}