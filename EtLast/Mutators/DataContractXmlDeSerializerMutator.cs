using System.Runtime.Serialization;
using System.Xml;

namespace FizzCode.EtLast;

public sealed class DataContractXmlDeSerializerMutator<T> : AbstractMutator
{
    public string SourceColumn { get; init; }
    public string TargetColumn { get; init; }

    public InvalidValueAction ActionIfFailed { get; init; }
    public object SpecialValueIfFailed { get; init; }

    public DataContractXmlDeSerializerMutator(IEtlContext context)
        : base(context)
    {
    }

    protected override IEnumerable<IRow> MutateRow(IRow row)
    {
        var sourceByteArray = row.GetAs<byte[]>(SourceColumn);
        if (sourceByteArray == null)
        {
            yield return row;
            yield break;
        }

        var removeRow = false;
        try
        {
            using (var ms = new MemoryStream(sourceByteArray))
            {
                object obj = null;
                using (var reader = XmlDictionaryReader.CreateTextReader(sourceByteArray, XmlDictionaryReaderQuotas.Max))
                {
                    var ser = new DataContractSerializer(typeof(T));
                    obj = ser.ReadObject(reader, true);
                }

                row[TargetColumn] = obj;
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
                    throw new ProcessExecutionException(this, row, "DataContract XML deserialization failed", ex);
                case InvalidValueAction.RemoveRow:
                    removeRow = true;
                    break;
                case InvalidValueAction.WrapError:
                    row[TargetColumn] = new EtlRowError
                    {
                        Process = this,
                        OriginalValue = null,
                        Message = "DataContract XML deserialization failed: " + ex.Message,
                    };
                    break;
            }
        }

        if (!removeRow)
            yield return row;
    }

    protected override void ValidateMutator()
    {
        if (SourceColumn == null)
            throw new ProcessParameterNullException(this, nameof(SourceColumn));

        if (TargetColumn == null)
            throw new ProcessParameterNullException(this, nameof(TargetColumn));
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class DataContractXmlDeSerializerMutatorFluent
{
    public static IFluentProcessMutatorBuilder DeSerializeFromXml<T>(this IFluentProcessMutatorBuilder builder, DataContractXmlDeSerializerMutator<T> mutator)
    {
        return builder.AddMutator(mutator);
    }
}
