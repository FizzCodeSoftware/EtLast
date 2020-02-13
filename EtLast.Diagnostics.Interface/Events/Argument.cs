namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Globalization;
    using System.Linq;
    using System.Text.Json.Serialization;

    public class Argument
    {
        [JsonPropertyName("v")]
        public string TransportValue { get; set; }

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
                ArgumentType._bool => TransportValue == "1",
                ArgumentType._char => TransportValue[0],
                ArgumentType._sbyte => sbyte.Parse(TransportValue, CultureInfo.InvariantCulture),
                ArgumentType._byte => byte.Parse(TransportValue, CultureInfo.InvariantCulture),
                ArgumentType._short => short.Parse(TransportValue, CultureInfo.InvariantCulture),
                ArgumentType._ushort => ushort.Parse(TransportValue, CultureInfo.InvariantCulture),
                ArgumentType._int => int.Parse(TransportValue, CultureInfo.InvariantCulture),
                ArgumentType._uint => uint.Parse(TransportValue, CultureInfo.InvariantCulture),
                ArgumentType._long => long.Parse(TransportValue, CultureInfo.InvariantCulture),
                ArgumentType._ulong => ulong.Parse(TransportValue, CultureInfo.InvariantCulture),
                ArgumentType._float => float.Parse(TransportValue, CultureInfo.InvariantCulture),
                ArgumentType._double => double.Parse(TransportValue, CultureInfo.InvariantCulture),
                ArgumentType._decimal => decimal.Parse(TransportValue, CultureInfo.InvariantCulture),
                ArgumentType._datetime => new DateTime(long.Parse(TransportValue, CultureInfo.InvariantCulture)),
                ArgumentType._datetimeoffset => DateTimeOffsetFromTextValue(),
                ArgumentType._timespan => TimeSpan.FromMilliseconds(long.Parse(TransportValue, CultureInfo.InvariantCulture)),
                ArgumentType._stringArray => TransportValue.Split("\0"),
                _ => TransportValue,
            };

            TransportValue = null;
        }

        private DateTimeOffset DateTimeOffsetFromTextValue()
        {
            var parts = TransportValue.Split("|");
            return new DateTimeOffset(long.Parse(parts[0], CultureInfo.InvariantCulture), new TimeSpan(long.Parse(parts[1], CultureInfo.InvariantCulture)));
        }

        public string ToDisplayValue()
        {
            if (Value == null)
                return "NULL";

            return Value switch
            {
                bool v => v ? "true" : "false",
                char v => "\'" + v.ToString(CultureInfo.InvariantCulture) + "\'",
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
                _ => TransportValue,
            };
        }

        public static string LongToString(long value)
        {
            return value.ToString("#,0", CultureInfo.InvariantCulture);
        }

        public static string TimeSpanToString(TimeSpan value, bool detailedMilliseconds = true)
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
            else
            {
                return value.ToString(@"s\.f" + (detailedMilliseconds ? "ff" : ""), CultureInfo.InvariantCulture);
            }
        }

        public void CalculateTextValue()
        {
            if (Value == null)
            {
                TransportValue = null;
                Type = ArgumentType._other;
                return;
            }

            if (Value is EtlRowError rowErr)
            {
                TransportValue = rowErr.Message;
                Type = ArgumentType._error;
                return;
            }

            if (Value is string sv)
            {
                TransportValue = sv;
                Type = ArgumentType._string;
                return;
            }

            if (Value is bool bv)
            {
                TransportValue = bv ? "1" : "0";
                Type = ArgumentType._bool;
                return;
            }

            if (Value is char cv)
            {
                TransportValue = cv.ToString();
                Type = ArgumentType._char;
                return;
            }

            if (Value is sbyte sbytev)
            {
                TransportValue = sbytev.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._sbyte;
                return;
            }

            if (Value is byte bytev)
            {
                TransportValue = bytev.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._byte;
                return;
            }

            if (Value is short shortv)
            {
                TransportValue = shortv.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._short;
                return;
            }

            if (Value is short ushortv)
            {
                TransportValue = ushortv.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._ushort;
                return;
            }

            if (Value is int iv)
            {
                TransportValue = iv.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._int;
                return;
            }

            if (Value is uint uintv)
            {
                TransportValue = uintv.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._uint;
                return;
            }

            if (Value is long lv)
            {
                TransportValue = lv.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._long;
                return;
            }

            if (Value is ulong ulongv)
            {
                TransportValue = ulongv.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._ulong;
                return;
            }

            if (Value is float fv)
            {
                TransportValue = fv.ToString("G", CultureInfo.InvariantCulture);
                Type = ArgumentType._float;
                return;
            }

            if (Value is double dv)
            {
                TransportValue = dv.ToString("G", CultureInfo.InvariantCulture);
                Type = ArgumentType._double;
                return;
            }

            if (Value is decimal dev)
            {
                TransportValue = dev.ToString("G", CultureInfo.InvariantCulture);
                Type = ArgumentType._decimal;
                return;
            }

            if (Value is TimeSpan ts)
            {
                TransportValue = Convert.ToInt64(ts.TotalMilliseconds).ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._timespan;
                return;
            }

            if (Value is DateTime dt)
            {
                TransportValue = dt.Ticks.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._datetime;
                return;
            }

            if (Value is DateTimeOffset dto)
            {
                TransportValue = dto.Ticks.ToString("D", CultureInfo.InvariantCulture) + "|" + dto.Offset.Ticks.ToString("D", CultureInfo.InvariantCulture);
                Type = ArgumentType._datetimeoffset;
                return;
            }

            if (Value is string[] strArr)
            {
                TransportValue = string.Join("\0", strArr);
                Type = ArgumentType._stringArray;
                return;
            }

            var valueType = Value.GetType();
            if (valueType.IsClass)
            {
                TransportValue = valueType.Name;
                Type = ArgumentType._other;
                return;
            }

            TransportValue = Value.ToString();
            Type = ArgumentType._other;
        }
    }
}