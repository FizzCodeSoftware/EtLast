using System;

namespace FizzCode.EtLast
{
    public static class ReaderProcessHelper
    {
        public static object HandleConverter(IProcess process, object value, int rowIndex, string rowColumn, ReaderDefaultColumnConfiguration configuration, IRow row, out bool failed)
        {
            failed = false;

            if (value == null)
            {
                switch (configuration.NullSourceHandler)
                {
                    case NullSourceHandler.WrapError:
                        row.SetValue(rowColumn, new EtlRowError()
                        {
                            Process = process,
                            Operation = null,
                            OriginalValue = null,
                            Message = string.Format("failed to convert by {0}", configuration.Converter.GetType().Name),
                        }, process);
                        failed = true;
                        return value;
                    case NullSourceHandler.SetSpecialValue:
                        return configuration.SpecialValueIfSourceIsNull;
                    case NullSourceHandler.Throw:
                        throw new InvalidValueException(process, row, rowColumn);
                    default:
                        throw new NotImplementedException(configuration.NullSourceHandler.ToString() + " is not supported yet");
                }
            }

            if (value != null && configuration.Converter != null)
            {
                var newValue = configuration.Converter.Convert(value);
                if (newValue != null)
                    return newValue;

                //process.Context.Log(LogSeverity.Debug, process, "failed converting '{OriginalColumn}' in row #{RowIndex}: '{ValueAsString}' ({ValueType}) using {ConverterType}", rowColumn, rowIndex, value.ToString(), value.GetType().Name, configuration.Converter.GetType().Name);

                switch (configuration.InvalidSourceHandler)
                {
                    case InvalidSourceHandler.WrapError:
                        row.SetValue(rowColumn, new EtlRowError()
                        {
                            Process = process,
                            Operation = null,
                            OriginalValue = value,
                            Message = string.Format("failed to convert by {0}", configuration.Converter.GetType().Name),
                        }, process);
                        break;
                    case InvalidSourceHandler.SetSpecialValue:
                        row.SetValue(rowColumn, configuration.SpecialValueIfSourceIsInvalid, process);
                        break;
                    case InvalidSourceHandler.Throw:
                        throw new InvalidValueException(process, row, rowColumn);
                    default:
                        throw new NotImplementedException(configuration.NullSourceHandler.ToString() + " is not supported yet");
                }

                failed = true;
                return value;
            }

            return value;
        }
    }
}