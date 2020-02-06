namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Globalization;
    using System.Linq;
    using System.Text.Json.Serialization;

    public class Argument
    {
        [JsonPropertyName("v")]
        public string TextValue { get; set; }

        [JsonPropertyName("t")]
        public ArgumentType Type { get; set; }

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
                ArgumentType._bool => TextValue == "1",
                ArgumentType._char => TextValue[0],
                ArgumentType._sbyte => sbyte.Parse(TextValue, CultureInfo.InvariantCulture),
                ArgumentType._byte => byte.Parse(TextValue, CultureInfo.InvariantCulture),
                ArgumentType._short => short.Parse(TextValue, CultureInfo.InvariantCulture),
                ArgumentType._ushort => ushort.Parse(TextValue, CultureInfo.InvariantCulture),
                ArgumentType._int => int.Parse(TextValue, CultureInfo.InvariantCulture),
                ArgumentType._uint => uint.Parse(TextValue, CultureInfo.InvariantCulture),
                ArgumentType._long => long.Parse(TextValue, CultureInfo.InvariantCulture),
                ArgumentType._ulong => ulong.Parse(TextValue, CultureInfo.InvariantCulture),
                ArgumentType._float => float.Parse(TextValue, CultureInfo.InvariantCulture),
                ArgumentType._double => double.Parse(TextValue, CultureInfo.InvariantCulture),
                ArgumentType._decimal => decimal.Parse(TextValue, CultureInfo.InvariantCulture),
                ArgumentType._datetime => new DateTime(long.Parse(TextValue, CultureInfo.InvariantCulture)),
                ArgumentType._datetimeoffset => DateTimeOffsetFromTextValue(),
                ArgumentType._timespan => TimeSpan.FromMilliseconds(long.Parse(TextValue, CultureInfo.InvariantCulture)),
                ArgumentType._stringArray => TextValue.Split("\0"),
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
                bool v => v ? "true" : "false",
                char v => "\'" + v + "\'",
                string v => "\"" + v + "\"",
                string[] v => string.Join(", ", v.Select(x => "\"" + x + "\"")),
                sbyte v => v.ToString("#,0", CultureInfo.InvariantCulture),
                byte v => v.ToString("#,0", CultureInfo.InvariantCulture),
                short v => v.ToString("#,0", CultureInfo.InvariantCulture),
                ushort v => v.ToString("#,0", CultureInfo.InvariantCulture),
                int v => v.ToString("#,0", CultureInfo.InvariantCulture),
                uint v => v.ToString("#,0", CultureInfo.InvariantCulture),
                long v => LongToString(v),
                ulong v => v.ToString("#,0", CultureInfo.InvariantCulture),
                float v => v.ToString("#,0.#", CultureInfo.InvariantCulture),
                double v => v.ToString("#,0.#", CultureInfo.InvariantCulture),
                decimal v => v.ToString("#,0.#", CultureInfo.InvariantCulture),
                TimeSpan v => TimeSpanToString(v),
                DateTime v => v.ToString("yyyy.MM.dd HH:mm:ss.fff", CultureInfo.InvariantCulture),
                DateTimeOffset v => v.ToString("yyyy.MM.dd HH:mm:ss.fff zzz", CultureInfo.InvariantCulture),
                _ => TextValue,
            };
        }

        public static string LongToString(long value)
        {
            return value.ToString("#,0", CultureInfo.InvariantCulture);
        }

        public static string TimeSpanToString(TimeSpan value)
        {
            if (value.Days > 0)
            {
                return value.ToString(@"d\.hh\:mm", CultureInfo.InvariantCulture);
            }
            else if (value.Hours > 0)
            {
                return value.ToString(@"h\:mm\:ss", CultureInfo.InvariantCulture);
            }
            else if (value.Minutes > 0)
            {
                return value.ToString(@"m\:ss", CultureInfo.InvariantCulture);
            }
            else if (value.Seconds > 0)
            {
                return value.ToString(@"s\.fff", CultureInfo.InvariantCulture);
            }

            return value.ToString(@"\.fff", CultureInfo.InvariantCulture);
        }

        public void CalculateTextValue()
        {
            if (Value == null)
            {
                TextValue = null;
                Type = ArgumentType._other;
                return;
            }

            if (Value is EtlRowError rowErr)
            {
                TextValue = rowErr.Message;
                Type = ArgumentType._error;
                return;
            }

            if (Value is string sv)
            {
                TextValue = sv;
                Type = ArgumentType._string;
                return;
            }

            if (Value is bool bv)
            {
                TextValue = bv ? "1" : "0";
                Type = ArgumentType._bool;
                return;
            }

            if (Value is char cv)
            {
                TextValue = cv.ToString();
                Type = ArgumentType._char;
                return;
            }

            if (Value is sbyte sbytev)
            {
                TextValue = sbytev.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._sbyte;
                return;
            }

            if (Value is byte bytev)
            {
                TextValue = bytev.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._byte;
                return;
            }

            if (Value is short shortv)
            {
                TextValue = shortv.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._short;
                return;
            }

            if (Value is short ushortv)
            {
                TextValue = ushortv.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._ushort;
                return;
            }

            if (Value is int iv)
            {
                TextValue = iv.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._int;
                return;
            }

            if (Value is uint uintv)
            {
                TextValue = uintv.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._uint;
                return;
            }

            if (Value is long lv)
            {
                TextValue = lv.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._long;
                return;
            }

            if (Value is ulong ulongv)
            {
                TextValue = ulongv.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._ulong;
                return;
            }

            if (Value is float fv)
            {
                TextValue = fv.ToString("G", CultureInfo.InvariantCulture);
                Type = ArgumentType._float;
                return;
            }

            if (Value is double dv)
            {
                TextValue = dv.ToString("G", CultureInfo.InvariantCulture);
                Type = ArgumentType._double;
                return;
            }

            if (Value is decimal dev)
            {
                TextValue = dev.ToString("G", CultureInfo.InvariantCulture);
                Type = ArgumentType._decimal;
                return;
            }

            if (Value is TimeSpan ts)
            {
                TextValue = Convert.ToInt64(ts.TotalMilliseconds).ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._timespan;
                return;
            }

            if (Value is DateTime dt)
            {
                TextValue = dt.Ticks.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._datetime;
                return;
            }

            if (Value is DateTimeOffset dto)
            {
                TextValue = dto.Ticks.ToString("D", CultureInfo.InvariantCulture) + "|" + dto.Offset.Ticks.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._datetimeoffset;
                return;
            }

            if (Value is string[] strArr)
            {
                TextValue = string.Join("\0", strArr);
                Type = ArgumentType._stringArray;
                return;
            }

            var valueType = Value.GetType();
            if (valueType.IsClass)
            {
                TextValue = valueType.Name;
                Type = ArgumentType._other;
                return;
            }

            TextValue = Value.ToString();
            Type = ArgumentType._other;
        }
    }
}