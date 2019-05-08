namespace FizzCode.EtLast
{
    public static class ReaderProcessHelper
    {
        public static object HandleConverter(IProcess process, object value, int rowIndex, string column, ReaderDefaultColumnConfiguration configuration, IRow row, out bool failed)
        {
            failed = false;

            if (value == null && configuration.ValueIfSourceIsNull != null)
            {
                return configuration.ValueIfSourceIsNull;
            }

            if (value != null && configuration.Converter != null)
            {
                var newValue = configuration.Converter.Convert(value);
                if (newValue != null)
                    return newValue;

                process.Context.Log(LogSeverity.Debug, process, "failed converting '{OriginalColumn}' in row #{RowIndex}: '{ValueAsString}' ({ValueType}) using {ConverterType}", column, rowIndex, value.ToString(), value.GetType().Name, configuration.Converter.GetType().Name);

                if (configuration.ValueIfConversionFailed == null)
                {
                    row.SetValue(column, new EtlRowError()
                    {
                        Process = process,
                        Operation = null,
                        OriginalValue = value,
                        Message = string.Format("failed to convert by {0}", configuration.Converter.GetType().Name),
                    }, process);
                }
                else
                {
                    row.SetValue(column, configuration.ValueIfConversionFailed, process);
                }

                failed = true;
                return value;
            }

            return value;
        }
    }
}