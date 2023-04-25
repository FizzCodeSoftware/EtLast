namespace FizzCode.EtLast;

public sealed class ResolveHierarchyMutator : AbstractSimpleChangeMutator
{
    public required string IdentityColumn { get; init; }
    public required string[] LevelColumns { get; init; }
    public required string NewColumnWithParentId { get; init; }
    public required string NewColumnWithName { get; init; }
    public required string NewColumnWithLevel { get; init; }

    /// <summary>
    /// Default value is false.
    /// </summary>
    public required bool RemoveLevelColumns { get; init; }

    private object[] _lastIdOfLevel;

    public ResolveHierarchyMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override void StartMutator()
    {
        base.StartMutator();
        _lastIdOfLevel = new object[LevelColumns.Length];
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        Changes.Clear();

        for (var level = LevelColumns.Length - 1; level >= 0; level--)
        {
            var levelColumn = LevelColumns[level];

            var name = row.GetAs<string>(levelColumn);
            if (!string.IsNullOrEmpty(name))
            {
                _lastIdOfLevel[level] = row[IdentityColumn];

                if (!string.IsNullOrEmpty(NewColumnWithParentId) && level > 0)
                {
                    Changes.Add(new KeyValuePair<string, object>(NewColumnWithParentId, _lastIdOfLevel[level - 1]));
                }

                if (!string.IsNullOrEmpty(NewColumnWithLevel))
                {
                    Changes.Add(new KeyValuePair<string, object>(NewColumnWithLevel, level));
                }

                if (!string.IsNullOrEmpty(NewColumnWithName))
                {
                    Changes.Add(new KeyValuePair<string, object>(NewColumnWithName, name));
                }

                break;
            }
        }

        if (RemoveLevelColumns)
        {
            foreach (var levelColumn in LevelColumns)
            {
                Changes.Add(new KeyValuePair<string, object>(levelColumn, null));
            }
        }

        row.MergeWith(Changes);
        yield return row;
    }

    public override void ValidateParameters()
    {
        if (string.IsNullOrEmpty(IdentityColumn))
            throw new ProcessParameterNullException(this, nameof(IdentityColumn));

        if (LevelColumns == null || LevelColumns.Length == 0)
            throw new ProcessParameterNullException(this, nameof(LevelColumns));
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class HierarchyParentIdCalculatorMutatorFluent
{
    public static IFluentSequenceMutatorBuilder ResolveHierarchy(this IFluentSequenceMutatorBuilder builder, ResolveHierarchyMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
