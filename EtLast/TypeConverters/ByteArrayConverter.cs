namespace FizzCode.EtLast;

public class ByteArrayConverter : ITypeConverter
{
    public virtual object Convert(object source)
    {
        if (source is byte[])
            return source;

        return null;
    }
}

[EditorBrowsable(EditorBrowsableState.Never)]
public static class ByteArrayConverterFluent
{
    public static ReaderColumn AsByteArray(this ReaderColumn column) => column.WithTypeConverter(new ByteArrayConverter());
    public static IConvertMutatorBuilder_NullStrategy ToByteArray(this IConvertMutatorBuilder_WithTypeConverter builder) => builder.WithTypeConverter(new ByteArrayConverter());
}