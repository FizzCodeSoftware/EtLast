namespace FizzCode.EtLast
{
    using System.Collections.Generic;
    using System.Globalization;

    public class InPlaceConvertMutator : AbstractMutator
    {
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

        public InPlaceConvertMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var removeRow = false;

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
                            throw new InvalidValueException(this, TypeConverter, row, column);
                        case InvalidValueAction.RemoveRow:
                            removeRow = true;
                            break;
                        case InvalidValueAction.WrapError:
                            row.SetValue(column, new EtlRowError
                            {
                                Process = this,
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
                        throw new InvalidValueException(this, TypeConverter, row, column);
                    case InvalidValueAction.RemoveRow:
                        removeRow = true;
                        break;
                    case InvalidValueAction.WrapError:
                        row.SetValue(column, new EtlRowError
                        {
                            Process = this,
                            OriginalValue = source,
                            Message = string.Format(CultureInfo.InvariantCulture, "invalid source detected by {0}", Name),
                        }, this);
                        break;
                }
            }

            if (!removeRow)
                yield return row;
        }

        protected override void ValidateMutator()
        {
            if (TypeConverter == null)
                throw new ProcessParameterNullException(this, nameof(TypeConverter));

            if (Columns.Length == 0)
                throw new ProcessParameterNullException(this, nameof(Columns));

            if (ActionIfInvalid != InvalidValueAction.SetSpecialValue && SpecialValueIfInvalid != null)
                throw new ProcessParameterNullException(this, nameof(SpecialValueIfInvalid));
        }
    }
}