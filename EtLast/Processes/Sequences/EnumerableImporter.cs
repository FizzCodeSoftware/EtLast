namespace FizzCode.EtLast;

public delegate IEnumerable<IReadOnlySlimRow> EnumerableImporterDelegate(EnumerableImporter process);

public sealed class EnumerableImporter : AbstractRowSource
{
    public required EnumerableImporterDelegate InputGenerator { get; init; }

    public Dictionary<string, ReaderColumn> Columns { get; set; }
    public ReaderColumn DefaultColumns { get; set; }

    /// <summary>
    /// Default false.
    /// </summary>
    public bool CopyOnlySpecifiedColumns { get; set; }

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
                    if (FlowState.IsTerminating)
                        yield break;

                    foreach (var columnKvp in Columns)
                    {
                        var value = inputRow[columnKvp.Value.SourceColumn ?? columnKvp.Key];
                        try
                        {
                            initialValues[columnKvp.Key] = columnKvp.Value.Process(this, value);
                        }
                        catch (Exception ex)
                        {
                            initialValues[columnKvp.Key] = new EtlRowError(this, value, ex);
                        }
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
                    ? new Dictionary<string, ReaderColumn>(Columns, StringComparer.InvariantCultureIgnoreCase)
                    : null;

                foreach (var inputRow in inputRows)
                {
                    if (FlowState.IsTerminating)
                        yield break;

                    foreach (var columnKvp in Columns)
                    {
                        var value = inputRow[columnKvp.Value.SourceColumn ?? columnKvp.Key];
                        try
                        {
                            initialValues[columnKvp.Key] = columnKvp.Value.Process(this, value);
                        }
                        catch (Exception ex)
                        {
                            initialValues[columnKvp.Key] = new EtlRowError(this, value, ex);
                        }
                    }

                    foreach (var valueKvp in inputRow.Values)
                    {
                        if (!columnMap.ContainsKey(valueKvp.Key.ToUpperInvariant()))
                        {
                            if (DefaultColumns != null)
                            {
                                try
                                {
                                    initialValues[valueKvp.Key] = DefaultColumns.Process(this, valueKvp.Value);
                                }
                                catch (Exception ex)
                                {
                                    initialValues[valueKvp.Key] = new EtlRowError(this, valueKvp.Value, ex);
                                }
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
                if (FlowState.IsTerminating)
                    yield break;

                yield return Context.CreateRow(this, row);
            }
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class EnumerableImporterFluent
{
    public static IFluentSequenceMutatorBuilder ImportEnumerable(this IFluentSequenceBuilder builder, EnumerableImporter importer)
    {
        return builder.ReadFrom(importer);
    }
}