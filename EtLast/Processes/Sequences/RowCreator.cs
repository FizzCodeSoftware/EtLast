namespace FizzCode.EtLast;

/// <summary>
/// Creates <see cref="IRow"/>s from <see cref="InputRows"/>.
/// Use to create test rows, or to produce fixed data.
/// </summary>
public sealed class RowCreator : AbstractRowSource
{
    public required string[] Columns { get; init; }
    public required List<object[]> InputRows { get; init; }

    protected override void ValidateImpl()
    {
        if (InputRows == null)
            throw new ProcessParameterNullException(this, nameof(InputRows));

        if (Columns == null)
            throw new ProcessParameterNullException(this, nameof(Columns));
    }

    protected override IEnumerable<IRow> Produce()
    {
        foreach (var inputRow in InputRows)
        {
            if (FlowState.IsTerminating)
                yield break;

            var initialValues = Enumerable
                .Range(0, Math.Min(Columns.Length, inputRow.Length))
                .Select(i => new KeyValuePair<string, object>(Columns[i], inputRow[i]))
                .ToList();

            yield return Context.CreateRow(this, initialValues);
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class RowCreatorFluent
{
    public static IFluentSequenceMutatorBuilder UsePredefinedRows(this IFluentSequenceBuilder builder, RowCreator creator)
    {
        return builder.ReadFrom(creator);
    }
}