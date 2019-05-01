namespace FizzCode.EtLast
{
    public static class ReaderProcessHelper
    {
        public static object HandleConverter(IProcess process, object value, string rowColumn, (int RowIndex, ITypeConverter Converter, object ValueIfNull) columnIndex, IRow row, out bool shouldContinue)
        {
            shouldContinue = false;
            // Converting if has value
            if (value == null || columnIndex.Converter == null)
            {
                if (value == null && columnIndex.ValueIfNull != null)
                {
                    value = columnIndex.ValueIfNull;
                }
            }
            else
            {
                var newValue = columnIndex.Converter.Convert(value);
                if (newValue != null)
                {
                    value = newValue;
                }
                else
                {
                    process.Context.Log(LogSeverity.Debug, process, "failed converting '{OriginalColumn}' in row #{RowIndex}: '{ValueAsString}' ({ValueType}) using {ConverterType}", rowColumn, columnIndex.RowIndex, value.ToString(), value.GetType().Name, columnIndex.Converter.GetType().Name);
                    
                    row.SetValue(rowColumn, new EtlRowError()
                    {
                        Process = process,
                        Operation = null,
                        OriginalValue = value,
                        Message = string.Format("failed to convert by {0}", columnIndex.Converter.GetType().Name),
                    }, process);

                    shouldContinue = true;
                    return value;
                }
            }

            return value;
        }
    }
}
