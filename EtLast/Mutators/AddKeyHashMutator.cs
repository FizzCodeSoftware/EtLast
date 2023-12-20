namespace FizzCode.EtLast;

public sealed class AddKeyHashMutator : AbstractMutator
{
    [ProcessParameterMustHaveValue]
    public required string TargetColumn { get; init; }

    /// <summary>
    /// Creates the hash algorithm used by this mutator. Recommendation is <see cref="SHA256.Create()"/>.
    /// </summary>
    [ProcessParameterMustHaveValue]
    public required Func<HashAlgorithm> HashAlgorithmCreator { get; init; }

    public string[] KeyColumns { get; init; }

    /// <summary>
    /// Default value is false.
    /// </summary>
    public bool IgnoreKeyCase { get; set; }

    /// <summary>
    /// Default value is false.
    /// </summary>
    public bool UpperCaseHash { get; set; }

    private HashAlgorithm _hashAlgorithm;
    private StringBuilder _hashStringBuilder;

    protected override void CloseMutator()
    {
        if (_hashAlgorithm != null)
        {
            _hashAlgorithm.Dispose();
            _hashAlgorithm = null;
        }
    }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        var columns = KeyColumns
            ?? row.Values.Select(x => x.Key).ToArray();

        var key = IgnoreKeyCase
            ? row.GenerateKeyUpper(columns)
            : row.GenerateKey(columns);

        if (key != null)
        {
            _hashAlgorithm ??= HashAlgorithmCreator.Invoke();

            var keyBytes = Encoding.UTF8.GetBytes(key);
            var hash = _hashAlgorithm.ComputeHash(keyBytes);

            _hashStringBuilder ??= new StringBuilder();

            for (var i = 0; i < hash.Length; i++)
            {
                _hashStringBuilder.Append(hash[i].ToString(UpperCaseHash ? "X2" : "x2"));
            }

            row[TargetColumn] = _hashStringBuilder.ToString();
            _hashStringBuilder.Clear();
        }

        yield return row;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class AddHashMutatorFluent
{
    public static IFluentSequenceMutatorBuilder AddKeyHash(this IFluentSequenceMutatorBuilder builder, AddKeyHashMutator mutator)
    {
        return builder.AddMutator(mutator);
    }

    public static IFluentSequenceMutatorBuilder AddKeyHash(this IFluentSequenceMutatorBuilder builder, string targetColumn, params string[] keyColumns)
    {
        return builder.AddMutator(new AddKeyHashMutator()
        {
            TargetColumn = targetColumn,
            KeyColumns = keyColumns,
            HashAlgorithmCreator = SHA256.Create,
        });
    }

    public static IFluentSequenceMutatorBuilder AddKeyHash(this IFluentSequenceMutatorBuilder builder, string targetColumn, Func<HashAlgorithm> hashAlgorithmCreator)
    {
        return builder.AddMutator(new AddKeyHashMutator()
        {
            TargetColumn = targetColumn,
            HashAlgorithmCreator = hashAlgorithmCreator,
        });
    }

    public static IFluentSequenceMutatorBuilder AddKeyHash(this IFluentSequenceMutatorBuilder builder, string targetColumn, Func<HashAlgorithm> hashAlgorithmCreator, params string[] keyColumns)
    {
        return builder.AddMutator(new AddKeyHashMutator()
        {
            TargetColumn = targetColumn,
            HashAlgorithmCreator = hashAlgorithmCreator,
            KeyColumns = keyColumns,
        });
    }
}