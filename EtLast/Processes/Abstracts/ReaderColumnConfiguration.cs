namespace FizzCode.EtLast
{
    using System.Diagnostics;

    public enum NullSourceHandler { SetSpecialValue, WrapError }
    public enum InvalidSourceHandler { SetSpecialValue, WrapError }

    [DebuggerDisplay("{" + nameof(GetDebuggerDisplay) + "(),nq}")]
    public class ReaderColumnConfiguration : ReaderDefaultColumnConfiguration
    {
        public string SourceColumn { get; }
        public string RowColumn { get; }

        public ReaderColumnConfiguration(string sourceColumn, ITypeConverter converter, NullSourceHandler nullSourceHandler = NullSourceHandler.SetSpecialValue, InvalidSourceHandler invalidSourceHandler = InvalidSourceHandler.WrapError)
            : base(converter, nullSourceHandler, invalidSourceHandler)
        {
            SourceColumn = sourceColumn;
        }

        public ReaderColumnConfiguration(string sourceColumn, string rowColumn, ITypeConverter converter, NullSourceHandler nullSourceHandler = NullSourceHandler.SetSpecialValue, InvalidSourceHandler invalidSourceHandler = InvalidSourceHandler.WrapError)
            : base(converter, nullSourceHandler, invalidSourceHandler)
        {
            SourceColumn = sourceColumn;
            RowColumn = rowColumn;
        }

        private string GetDebuggerDisplay()
        {
            return SourceColumn + (RowColumn != null ? " -> " + RowColumn : "") + (Converter != null ? " (" + Converter.GetType().GetFriendlyTypeName() + ")" : "");
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