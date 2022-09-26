using System.Runtime.Serialization;
using System.Xml;

namespace FizzCode.EtLast;

public sealed class DataContractXmlSerializerMutator<T> : AbstractMutator
{
    public string SourceColumn { get; init; }
    public string TargetColumn { get; init; }

    public InvalidValueAction ActionIfFailed { get; init; }
    public object SpecialValueIfFailed { get; init; }

    public DataContractXmlSerializerMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
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
                    row[TargetColumn] = new EtlRowError
                    {
                        Process = this,
                        OriginalValue = null,
                        Message = "DataContract XML serialization failed: " + ex.Message,
                    };
                    break;
            }
        }

        if (!removeRow)
            yield return row;
    }

    public override void ValidateParameters()
    {
        if (SourceColumn == null)
            throw new ProcessParameterNullException(this, nameof(SourceColumn));

        if (TargetColumn == null)
            throw new ProcessParameterNullException(this, nameof(TargetColumn));
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
