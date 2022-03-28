namespace FizzCode.EtLast;

public enum ColumnAlreadyExistsAction
{
    Skip,
    RemoveRow,
    Throw
}

public sealed class RenameColumnMutator : AbstractSimpleChangeMutator
{
    /// <summary>
    /// Key is current name, Value is new name.
    /// </summary>
    public Dictionary<string, string> Columns { get; init; }

    /// <summary>
    /// Default value is <see cref="ColumnAlreadyExistsAction.Throw"/>
    /// </summary>
    public ColumnAlreadyExistsAction ActionIfTargetValueExists { get; init; } = ColumnAlreadyExistsAction.Throw;

    public RenameColumnMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        Changes.Clear();

        var removeRow = false;
        foreach (var kvp in Columns)
        {
            if (row.HasValue(kvp.Value))
            {
                switch (ActionIfTargetValueExists)
                {
                    case ColumnAlreadyExistsAction.RemoveRow:
                        removeRow = true;
                        continue;
                    case ColumnAlreadyExistsAction.Skip:
                        continue;
                    case ColumnAlreadyExistsAction.Throw:
                        var exception = new ColumnRenameException(this, row, kvp.Key, kvp.Value);
                        throw exception;
                }
            }

            var value = row[kvp.Key];
            Changes.Add(new KeyValuePair<string, object>(kvp.Key, null));
            Changes.Add(new KeyValuePair<string, object>(kvp.Value, value));
        }

        if (!removeRow)
        {
            row.MergeWith(Changes);
            yield return row;
        }
    }

    protected override void ValidateMutator()
    {
        if (Columns == null)
            throw new ProcessParameterNullException(this, nameof(Columns));
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class RenameColumnMutatorFluent
{
    public static IFluentProcessMutatorBuilder RenameColumn(this IFluentProcessMutatorBuilder builder, RenameColumnMutator mutator)
    {
        return builder.AddMutator(mutator);
    }

    public static IFluentProcessMutatorBuilder RenameColumn(this IFluentProcessMutatorBuilder builder, string currentName, string newName)
    {
        return builder.AddMutator(new RenameColumnMutator(builder.ProcessBuilder.Result.Context)
        {
            Name = nameof(RenameColumn) + "From" + currentName + "To" + newName,
            Columns = new()
            {
                [currentName] = newName,
            }
        });
    }
}
