namespace FizzCode.EtLast;

using System.Collections.Generic;
using System.ComponentModel;

public sealed class SetTagMutator : AbstractMutator
{
    public object Tag { get; init; }

    public SetTagMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        row.Tag = Tag;
        yield return row;
    }

    protected override void ValidateMutator()
    {
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class SetTagMutatorFluent
{
    public static IFluentProcessMutatorBuilder SetTag(this IFluentProcessMutatorBuilder builder, SetTagMutator mutator)
    {
        return builder.AddMutator(mutator);
    }
}
