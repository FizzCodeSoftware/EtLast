namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System;
    using System.Globalization;
    using System.Text.Json.Serialization;

    public class NamedArgument
    {
        public string Name { get; set; }
        public string TextValue { get; set; }
        public string Type { get; set; }

        [JsonIgnore]
        public object Value { get; set; }

        public static NamedArgument FromObject(string name, object value)
        {
            var arg = new NamedArgument()
            {
                Name = name,
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
                "timespan" => TimeSpan.FromMilliseconds(long.Parse(TextValue, CultureInfo.InvariantCulture)),
                _ => TextValue,
            };
        }

        public void CalculateTextValue()
        {
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

            if (Value is TimeSpan ts)
            {
                TextValue = Convert.ToInt64(ts.TotalMilliseconds).ToString("D", CultureInfo.InvariantCulture);
                Type = "timespan";
                return;
            }

            TextValue = Value.ToString();
            Type = Value.GetType().Name;
        }
    }
}