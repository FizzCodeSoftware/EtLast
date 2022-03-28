namespace FizzCode.EtLast;

public delegate IEnumerable<IReadOnlySlimRow> EnumerableImporterDelegate(EnumerableImporter process);

public sealed class EnumerableImporter : AbstractRowSource
{
    public EnumerableImporterDelegate InputGenerator { get; set; }

    public Dictionary<string, ReaderColumnConfiguration> Columns { get; set; }
    public ReaderDefaultColumnConfiguration DefaultColumns { get; set; }

    /// <summary>
    /// Default false.
    /// </summary>
    public bool CopyOnlySpecifiedColumns { get; set; }

    public EnumerableImporter(IEtlContext context)
        : base(context)
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

        if (Columns != null)
        {
            var initialValues = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

            if (CopyOnlySpecifiedColumns)
            {
                foreach (var inputRow in inputRows)
                {
                    if (Context.CancellationTokenSource.IsCancellationRequested)
                        yield break;

                    foreach (var columnKvp in Columns)
                    {
                        var value = columnKvp.Value.Process(this, inputRow[columnKvp.Value.SourceColumn ?? columnKvp.Key]);
                        initialValues[columnKvp.Key] = value;
                    }

                    var newRow = Context.CreateRow(this, initialValues);
                    newRow.Tag = inputRow.Tag;

                    yield return newRow;
                    initialValues.Clear();
                }
            }
            else
            {
                var columnMap = Columns != null
                    ? new Dictionary<string, ReaderColumnConfiguration>(Columns, StringComparer.InvariantCultureIgnoreCase)
                    : null;

                foreach (var inputRow in inputRows)
                {
                    if (Context.CancellationTokenSource.IsCancellationRequested)
                        yield break;

                    foreach (var columnKvp in Columns)
                    {
                        var value = columnKvp.Value.Process(this, inputRow[columnKvp.Value.SourceColumn ?? columnKvp.Key]);
                        initialValues[columnKvp.Key] = value;
                    }

                    foreach (var valueKvp in inputRow.Values)
                    {
                        if (!columnMap.ContainsKey(valueKvp.Key.ToUpperInvariant()))
                        {
                            if (DefaultColumns != null)
                            {
                                var value = DefaultColumns.Process(this, valueKvp.Value);
                                initialValues[valueKvp.Key] = value;
                            }
                            else
                            {
                                initialValues[valueKvp.Key] = valueKvp.Value;
                            }
                        }
                    }

                    var newRow = Context.CreateRow(this, initialValues);
                    newRow.Tag = inputRow.Tag;

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
