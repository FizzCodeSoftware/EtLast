using System.Runtime.Serialization;
using System.Xml;

namespace FizzCode.EtLast;

public class DataContractXmlDeSerializerConverter<T> : ITypeConverter
{
    public virtual object Convert(object source)
    {
        byte[] sourceByteArray = null;
        if (source is byte[] sb)
            sourceByteArray = sb;
        else if (source is string str)
            sourceByteArray = Encoding.UTF8.GetBytes(str);

        if (sourceByteArray == null)
            return null;

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

                return obj;
            }
        }
        catch
        {
            return null;
        }
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class DataContractXmlDeSerializerConverterFluent
{
    public static ReaderColumn DeserializeDataContract<T>(this ReaderColumn column) => column.WithTypeConverter(new DataContractXmlDeSerializerConverter<T>());
    public static IConvertMutatorBuilder_NullStrategy DeserializeDataContractTo<T>(this IConvertMutatorBuilder_WithTypeConverter builder) => builder.WithTypeConverter(new DataContractXmlDeSerializerConverter<T>());
}