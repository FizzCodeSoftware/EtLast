using System.Runtime.Serialization;
using System.Xml;

namespace FizzCode.EtLast;

public class DataContractXmlSerializerConverter : ITypeConverter
{
    public virtual object Convert(object source)
    {
        try
        {
            using (var ms = new MemoryStream())
            {
                using (var writer = XmlDictionaryWriter.CreateTextWriter(ms, Encoding.UTF8, ownsStream: false))
                {
                    var ser = new DataContractSerializer(source.GetType());
                    ser.WriteObject(writer, source);
                }

                return Encoding.UTF8.GetString(ms.ToArray());
            }
        }
        catch
        {
            return null;
        }
    }
}

[Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
public static class DataContractXmlSerializerConverterFluent
{
    public static ReaderColumn SerializeDataContract(this ReaderColumn column) => column.WithTypeConverter(new DataContractXmlSerializerConverter());
    public static IConvertMutatorBuilder_NullStrategy SerializeToDataContract(this IConvertMutatorBuilder_WithTypeConverter builder) => builder.WithTypeConverter(new DataContractXmlSerializerConverter());
}