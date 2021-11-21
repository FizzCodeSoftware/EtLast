namespace FizzCode.EtLast
{
    public enum NullSourceHandler { SetSpecialValue, WrapError }
    public enum InvalidSourceHandler { SetSpecialValue, WrapError }

    public class ReaderColumnConfiguration : ReaderDefaultColumnConfiguration
    {
        public string RowColumn { get; }

        public ReaderColumnConfiguration(ITypeConverter converter, NullSourceHandler nullSourceHandler = NullSourceHandler.SetSpecialValue, InvalidSourceHandler invalidSourceHandler = InvalidSourceHandler.WrapError)
            : base(converter, nullSourceHandler, invalidSourceHandler)
        {
        }

        public ReaderColumnConfiguration(string rowColumn, ITypeConverter converter, NullSourceHandler nullSourceHandler = NullSourceHandler.SetSpecialValue, InvalidSourceHandler invalidSourceHandler = InvalidSourceHandler.WrapError)
            : base(converter, nullSourceHandler, invalidSourceHandler)
        {
            RowColumn = rowColumn;
        }
    }

    public class ReaderDefaultColumnConfiguration
    {
        public ITypeConverter Converter { get; }

        public NullSourceHandler NullSourceHandler { get; }
        public object SpecialValueIfSourceIsNull { get; init; }

        public InvalidSourceHandler InvalidSourceHandler { get; }
        public object SpecialValueIfSourceIsInvalid { get; init; }

        public ReaderDefaultColumnConfiguration(ITypeConverter converter, NullSourceHandler nullSourceHandler = NullSourceHandler.SetSpecialValue, InvalidSourceHandler invalidSourceHandler = InvalidSourceHandler.WrapError)
        {
            Converter = converter;
            NullSourceHandler = nullSourceHandler;
            InvalidSourceHandler = invalidSourceHandler;
        }
    }
}