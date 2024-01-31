namespace FizzCode.EtLast;

public class ByteArrayConverter : ITypeConverter, ITextConverter
{
    public virtual object Convert(object source)
    {
        if (source is byte[])
            return source;

        return null;
    }

    public object Convert(TextBuilder source)
    {
        var data = source.GetContentAsSpan();
        var bufferLength = data.Length * 6 / 8;
        var buffer = new Span<byte>(new byte[bufferLength]);
        if (System.Convert.TryFromBase64Chars(data, buffer, out var bytesCount))
        {
            return buffer[..bytesCount].ToArray();
        }

        return null;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class ByteArrayConverterFluent
{
    public static ReaderColumn AsByteArray(this ReaderColumn column) => column.WithTypeConverter(new ByteArrayConverter());
    public static TextReaderColumn AsByteArray(this TextReaderColumn column) => column.WithTypeConverter(new ByteArrayConverter());
    public static IConvertMutatorBuilder_NullStrategy ToByteArray(this IConvertMutatorBuilder_WithTypeConverter builder) => builder.WithTypeConverter(new ByteArrayConverter());
}