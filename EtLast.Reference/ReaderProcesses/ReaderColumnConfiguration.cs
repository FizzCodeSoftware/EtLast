namespace FizzCode.EtLast
{
    public class ReaderColumnConfiguration
    {
        public string SourceColumn { get; set; }
        public string RowColumn { get; set; }
        public ITypeConverter Converter { get; set; }
        public object ValueIfSourceIsNull { get; set; }
        public object ValueIfConversionFailed { get; set; }

        public ReaderColumnConfiguration(ITypeConverter converter, object valueIfSourceIsNull = null, object valueIfConversionFailed = null)
        {
            Converter = converter;
            ValueIfSourceIsNull = valueIfSourceIsNull;
            ValueIfConversionFailed = valueIfConversionFailed;
        }

        public ReaderColumnConfiguration(string sourceColumn, ITypeConverter converter, object valueIfSourceIsNull = null, object valueIfConversionFailed = null)
        {
            SourceColumn = sourceColumn;
            Converter = converter;
            ValueIfSourceIsNull = valueIfSourceIsNull;
            ValueIfConversionFailed = valueIfConversionFailed;
        }

        public ReaderColumnConfiguration(string sourceColumn, string rowColumn, ITypeConverter converter, object valueIfSourceIsNull = null, object valueIfConversionFailed = null)
        {
            SourceColumn = sourceColumn;
            RowColumn = rowColumn;
            Converter = converter;
            ValueIfSourceIsNull = valueIfSourceIsNull;
            ValueIfConversionFailed = valueIfConversionFailed;
        }
    }
}