using System.Globalization;

namespace FizzCode.EtLast
{
    public class InPlaceConvertOperation : AbstractRowOperation
    {
        public RowTestDelegate If { get; set; }
        public string[] Columns { get; set; }
        public ITypeConverter TypeConverter { get; set; }

        /// <summary>
        /// Default value is <see cref="InvalidValueAction.RemoveRow"/>
        /// </summary>
        public InvalidValueAction ActionIfNull { get; set; } = InvalidValueAction.SetSpecialValue;

        /// <summary>
        /// Default value is null,
        /// </summary>
        public object SpecialValueIfNull { get; set; }

        /// <summary>
        /// Default value is <see cref="InvalidValueAction.WrapError"/>
        /// </summary>
        public InvalidValueAction ActionIfInvalid { get; set; } = InvalidValueAction.WrapError;

        /// <summary>
        /// Default value is null,
        /// </summary>
        public object SpecialValueIfInvalid { get; set; }

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            foreach (var column in Columns)
            {
                var source = row[column];
                if (source != null)
                {
                    var value = TypeConverter.Convert(source);
                    if (value != null)
                    {
                        row.SetValue(column, value, this);
                        continue;
                    }
                }
                else
                {
                    switch (ActionIfNull)
                    {
                        case InvalidValueAction.SetSpecialValue:
                            row.SetValue(column, SpecialValueIfNull, this);
                            break;
                        case InvalidValueAction.Throw:
                            throw new InvalidValueException(Process, TypeConverter, row, column);
                        case InvalidValueAction.RemoveRow:
                            Process.RemoveRow(row, this);
                            return;
                        case InvalidValueAction.WrapError:
                            row.SetValue(column, new EtlRowError
                            {
                                Process = Process,
                                Operation = this,
                                OriginalValue = source,
                                Message = string.Format(CultureInfo.InvariantCulture, "null source detected by {0}", Name),
                            }, this);
                            break;
                    }

                    continue;
                }

                switch (ActionIfInvalid)
                {
                    case InvalidValueAction.SetSpecialValue:
                        row.SetValue(column, SpecialValueIfInvalid, this);
                        break;
                    case InvalidValueAction.Throw:
                        throw new InvalidValueException(Process, TypeConverter, row, column);
                    case InvalidValueAction.RemoveRow:
                        Process.RemoveRow(row, this);
                        return;
                    case InvalidValueAction.WrapError:
                        row.SetValue(column, new EtlRowError
                        {
                            Process = Process,
                            Operation = this,
                            OriginalValue = source,
                            Message = string.Format(CultureInfo.InvariantCulture, "invalid source detected by {0}", Name),
                        }, this);
                        break;
                }
            }
        }

        public override void Prepare()
        {
            if (TypeConverter == null)
                throw new OperationParameterNullException(this, nameof(TypeConverter));

            if (Columns.Length == 0)
                throw new OperationParameterNullException(this, nameof(Columns));

            if (ActionIfInvalid != InvalidValueAction.SetSpecialValue && SpecialValueIfInvalid != null)
                throw new OperationParameterNullException(this, nameof(SpecialValueIfInvalid));
        }
    }
}