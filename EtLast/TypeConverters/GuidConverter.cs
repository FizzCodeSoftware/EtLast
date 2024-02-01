namespace FizzCode.EtLast;
public class GuidConverter : ITypeConverter, ITextConverter
{
    public object Convert(object source)
    {
        if (source is Guid guid)
        {
            return guid;
        }

        if (source is string s && Guid.TryParse(s, out var value))
        {
            return value;
        }

        return null;
    }

    public object Convert(TextBuilder source)
    {
        var span = source.GetContentAsSpan();
        if (Guid.TryParse(span, out var value))
        {
            return value;
        }

        return null;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class GuidConverterFluent
{
    public static ReaderColumn AsGuid(this ReaderColumn column) => column.WithTypeConverter(new GuidConverter());
    public static TextReaderColumn AsGuid(this TextReaderColumn column) => column.WithTypeConverter(new GuidConverter());
    public static IConvertMutatorBuilder_NullStrategy ToGuid(this IConvertMutatorBuilder_WithTypeConverter builder) => builder.WithTypeConverter(new GuidConverter());
}

