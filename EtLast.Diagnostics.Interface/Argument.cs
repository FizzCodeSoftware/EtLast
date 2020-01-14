namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Globalization;
    using System.Text.Json.Serialization;

    public class Argument
    {
        public string TextValue { get; set; }
        public string Type { get; set; }

        [JsonIgnore]
        public object Value { get; set; }

        public static Argument FromObject(object value)
        {
            var arg = new Argument()
            {
                Value = value,
            };

            arg.CalculateTextValue();

            return arg;
        }

        public void CalculateValue()
        {
            Value = Type switch
            {
                "string" => TextValue,
                "row-error" => TextValue,
                "bool" => TextValue == "1",
                "int" => int.Parse(TextValue, CultureInfo.InvariantCulture),
                "long" => long.Parse(TextValue, CultureInfo.InvariantCulture),
                "float" => float.Parse(TextValue, CultureInfo.InvariantCulture),
                "double" => double.Parse(TextValue, CultureInfo.InvariantCulture),
                "decimal" => decimal.Parse(TextValue, CultureInfo.InvariantCulture),
                "datetime" => new DateTime(long.Parse(TextValue, CultureInfo.InvariantCulture)),
                "datetimeoffset" => DateTimeOffsetFromTextValue(),
                "timespan" => TimeSpan.FromMilliseconds(long.Parse(TextValue, CultureInfo.InvariantCulture)),
                _ => TextValue,
            };
        }

        private DateTimeOffset DateTimeOffsetFromTextValue()
        {
            var parts = TextValue.Split("|");
            return new DateTimeOffset(long.Parse(parts[0], CultureInfo.InvariantCulture), new TimeSpan(long.Parse(parts[1], CultureInfo.InvariantCulture)));
        }

        public string ToDisplayValue()
        {
            if (Value == null)
                return "NULL";

            return Value switch
            {
                bool bv => bv ? "true" : "false",
                int iv => iv.ToString("#,0", CultureInfo.InvariantCulture),
                long lv => lv.ToString("#,0", CultureInfo.InvariantCulture),
                float fv => fv.ToString("#,0.#", CultureInfo.InvariantCulture),
                double dv => dv.ToString("#,0.#", CultureInfo.InvariantCulture),
                decimal decv => decv.ToString("#,0.#", CultureInfo.InvariantCulture),
                DateTime dt => dt.ToString("yyyy.MM.dd HH:mm:ss.fff", CultureInfo.InvariantCulture),
                DateTimeOffset dto => dto.ToString("yyyy.MM.dd HH:mm:ss.fff zzz", CultureInfo.InvariantCulture),
                TimeSpan ts => TimeSpanToString(ts),
                _ => TextValue,
            };
        }

        private string TimeSpanToString(TimeSpan ts)
        {
            if (ts.Days > 0)
            {
                return ts.ToString(@"d\.hh\:mm", CultureInfo.InvariantCulture);
            }
            else if (ts.Hours > 0)
            {
                return ts.ToString(@"h\:mm\:ss", CultureInfo.InvariantCulture);
            }
            else if (ts.Minutes > 0)
            {
                return ts.ToString(@"m\:ss", CultureInfo.InvariantCulture);
            }
            else if (ts.Seconds > 0)
            {
                return ts.ToString(@"s\.fff", CultureInfo.InvariantCulture);
            }

            return ts.ToString(@"\.fff", CultureInfo.InvariantCulture);
        }

        public void CalculateTextValue()
        {
            if (Value == null)
            {
                TextValue = null;
                Type = "?";
                return;
            }

            if (Value is EtlRowError rowErr)
            {
                TextValue = rowErr.Message;
                Type = "row-error";
                return;
            }

            if (Value is string sv)
            {
                TextValue = sv;
                Type = "string";
                return;
            }

            if (Value is bool bv)
            {
                TextValue = bv ? "1" : "0";
                Type = "bool";
                return;
            }

            if (Value is int iv)
            {
                TextValue = iv.ToString("D", CultureInfo.InvariantCulture);
                Type = "int";
                return;
            }

            if (Value is long lv)
            {
                TextValue = lv.ToString("D", CultureInfo.InvariantCulture);
                Type = "long";
                return;
            }

            if (Value is float fv)
            {
                TextValue = fv.ToString("G", CultureInfo.InvariantCulture);
                Type = "float";
                return;
            }

            if (Value is double dv)
            {
                TextValue = dv.ToString("G", CultureInfo.InvariantCulture);
                Type = "double";
                return;
            }

            if (Value is decimal dev)
            {
                TextValue = dev.ToString("G", CultureInfo.InvariantCulture);
                Type = "decimal";
                return;
            }

            if (Value is DateTime dt)
            {
                TextValue = dt.Ticks.ToString("D", CultureInfo.InvariantCulture);
                Type = "datetime";
                return;
            }

            if (Value is DateTimeOffset dto)
            {
                TextValue = dto.Ticks.ToString("D", CultureInfo.InvariantCulture) + "|" + dto.Offset.Ticks.ToString("D", CultureInfo.InvariantCulture);
                Type = "datetimeoffset";
                return;
            }

            if (Value is TimeSpan ts)
            {
                TextValue = Convert.ToInt64(ts.TotalMilliseconds).ToString("D", CultureInfo.InvariantCulture);
                Type = "timespan";
                return;
            }

            var valueType = Value.GetType();
            if (valueType.IsClass)
            {
                TextValue = "...";
                Type = valueType.Name;
                return;
            }

            TextValue = Value.ToString();
            Type = valueType.Name;
        }
    }
}