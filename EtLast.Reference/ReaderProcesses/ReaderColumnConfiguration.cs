namespace FizzCode.EtLast
{
    public class ReaderColumnConfiguration : ReaderDefaultColumnConfiguration
    {
        public string SourceColumn { get; }
        public string RowColumn { get; }

        public ReaderColumnConfiguration(string sourceColumn, ITypeConverter converter, object valueIfSourceIsNull = null, object valueIfConversionFailed = null)
            : base(converter, valueIfSourceIsNull, valueIfConversionFailed)
        {
            SourceColumn = sourceColumn;
        }

        public ReaderColumnConfiguration(string sourceColumn, string rowColumn, ITypeConverter converter, object valueIfSourceIsNull = null, object valueIfConversionFailed = null)
            : base(converter, valueIfSourceIsNull, valueIfConversionFailed)
        {
            SourceColumn = sourceColumn;
            RowColumn = rowColumn;
        }
    }

    public class ReaderDefaultColumnConfiguration
    {
        public ITypeConverter Converter { get; }
        public object ValueIfSourceIsNull { get; }
        public object ValueIfConversionFailed { get; }

        public ReaderDefaultColumnConfiguration(ITypeConverter converter, object valueIfSourceIsNull = null, object valueIfConversionFailed = null)
        {
            Converter = converter;
            ValueIfSourceIsNull = valueIfSourceIsNull;
            ValueIfConversionFailed = valueIfConversionFailed;
        }
    }
}