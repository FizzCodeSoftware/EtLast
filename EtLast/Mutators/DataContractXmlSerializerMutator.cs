using System.Runtime.Serialization;
using System.Xml;

namespace FizzCode.EtLast;

public sealed class DataContractXmlSerializerMutator<T>: AbstractMutator
{
    [ProcessParameterMustHaveValue]
    public required string SourceColumn { get; init; }

    [ProcessParameterMustHaveValue]
    public required string TargetColumn { get; init; }

    public required InvalidValueAction ActionIfFailed { get; init; }
    public object SpecialValueIfFailed { get; init; }

    protected override IEnumerable<IRow> MutateRow(IRow row, long rowInputIndex)
    {
        var sourceObject = row.GetAs<T>(SourceColumn);
        if (sourceObject == null)
        {
            yield return row;
            yield break;
        }

        var removeRow = false;
        try
        {
            using (var ms = new MemoryStream())
            {
                using (var writer = XmlDictionaryWriter.CreateTextWriter(ms))
                {
                    var ser = new DataContractSerializer(typeof(T));
                    ser.WriteObject(writer, sourceObject);
                }

                var data = ms.ToArray();
                row[TargetColumn] = data;
            }
        }
        catch (Exception ex)
        {
            switch (ActionIfFailed)
            {
                case InvalidValueAction.SetSpecialValue:
                    row[TargetColumn] = SpecialValueIfFailed;
                    break;
                case InvalidValueAction.Throw:
                    throw new ProcessExecutionException(this, row, "DataContract XML serialization failed", ex);
                case InvalidValueAction.RemoveRow:
                    removeRow = true;
                    break;
                case InvalidValueAction.WrapError:
                    row[TargetColumn] = new EtlRowError(this, null, "DataContract XML serialization failed: " + ex.Message);
                    break;
            }
        }

        if (!removeRow)
            yield return row;
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class DataContractXmlSerializerMutatorFluent
{
    public static IFluentSequenceMutatorBuilder SerializeToXml<T>(this IFluentSequenceMutatorBuilder builder, DataContractXmlSerializerMutator<T> mutator)
    {
        return builder.AddMutator(mutator);
    }
}
