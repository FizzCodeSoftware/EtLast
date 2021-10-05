namespace FizzCode.EtLast
{
    using System.IO;
    using System.Runtime.Serialization;
    using System.Xml;

    public class DataContractXmlDeSerializerConverter<T> : ITypeConverter
    {
        public virtual object Convert(object source)
        {
            if (source is not byte[] sourceByteArray)
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
}