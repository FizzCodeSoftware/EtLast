namespace FizzCode.EtLast.PluginHost.SerilogSink
{
    using System;
    using System.Globalization;
    using System.IO;
    using Serilog.Events;

    internal class JsonValueFormatter : AbstractValueFormatter
    {
        private readonly DefaultValueFormatter _defaultValueFormatter;

        internal JsonValueFormatter(DefaultValueFormatter defaultValueFormatter)
        {
            _defaultValueFormatter = defaultValueFormatter;
        }

        public override void FormatScalarValue(LogEvent logEvent, TextWriter builder, ScalarValue value, string format, bool topLevelScalar)
        {
            if (topLevelScalar)
            {
                _defaultValueFormatter.FormatScalarValue(logEvent, builder, value, format, false);
                return;
            }

            switch (value.Value)
            {
                case null:
                    ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.NullValue, "NULL");
                    break;
                case string strv:
                    using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.StringValue))
                    {
                        Serilog.Formatting.Json.JsonValueFormatter.WriteQuotedJsonString(strv, builder);
                        break;
                    }
                case bool bv:
                    ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.BooleanValue, bv ? "true" : "false");
                    break;
                case char chv:
                    using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.ScalarValue))
                    {
                        Serilog.Formatting.Json.JsonValueFormatter.WriteQuotedJsonString(chv.ToString(), builder);
                        break;
                    }
                case DateTime dtv:
                    ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.ScalarValue, "\"" + dtv.ToString("O", CultureInfo.InvariantCulture) + "\"");
                    break;
                case DateTimeOffset dtov:
                    ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.ScalarValue, "\"" + dtov.ToString("O", CultureInfo.InvariantCulture) + "\"");
                    break;
                case sbyte _:
                case byte _:
                case short _:
                case ushort _:
                case int _:
                case uint _:
                case long _:
                case ulong _:
                case decimal _:
                    ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.NumberValue, ((IFormattable)value.Value).ToString(null, CultureInfo.InvariantCulture));
                    break;
                case double dv:
                    using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.NumberValue))
                    {
                        if (double.IsNaN(dv))
                        {
                            ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.NumberValue, "\"NAN\"");
                        }
                        else if (double.IsInfinity(dv))
                        {
                            ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.NumberValue, "\"INF\"");
                        }
                        else
                        {
                            builder.Write(dv.ToString("G17", CultureInfo.InvariantCulture));
                        }

                        break;
                    }
                case float fv:
                    {
                        if (double.IsNaN(fv))
                        {
                            ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.NumberValue, "\"NAN\"");
                        }
                        else if (double.IsInfinity(fv))
                        {
                            ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.NumberValue, "\"INF\"");
                        }
                        else
                        {
                            ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.NumberValue, fv.ToString("G17", CultureInfo.InvariantCulture));
                        }

                        break;
                    }
                default:
                    {
                        using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.ScalarValue))
                        {
                            Serilog.Formatting.Json.JsonValueFormatter.WriteQuotedJsonString(value.Value.ToString(), builder);
                        }

                        break;
                    }
            }
        }

        public override void FormatStructureValue(LogEvent logEvent, TextWriter builder, StructureValue value, string format)
        {
            ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, "{");
            var isFirst = true;
            foreach (var property in value.Properties)
            {
                if (!isFirst)
                    ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, ", ");

                isFirst = false;

                using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.StructureName))
                {
                    Serilog.Formatting.Json.JsonValueFormatter.WriteQuotedJsonString(property.Name, builder);
                }

                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, ": ");

                Format(logEvent, property.Value, builder, null);
            }

            if (value.TypeTag != null)
            {
                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, ", ");

                using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.StructureName))
                {
                    Serilog.Formatting.Json.JsonValueFormatter.WriteQuotedJsonString("$type", builder);
                }

                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, ": ");

                using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.StringValue))
                {
                    Serilog.Formatting.Json.JsonValueFormatter.WriteQuotedJsonString(value.TypeTag, builder);
                }
            }

            ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, "}");
        }

        public override void FormatDictionaryValue(LogEvent logEvent, TextWriter builder, DictionaryValue value, string format)
        {
            ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, "{");

            var isFirst = true;
            foreach (var element in value.Elements)
            {
                if (!isFirst)
                    ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, ", ");

                isFirst = false;

                var colorCode = element.Key.Value == null
                    ? ColorCode.NullValue
                    : element.Key.Value is string
                        ? ColorCode.StringValue
                        : ColorCode.ScalarValue;

                using (ColorCodeContext.StartOverridden(builder, logEvent, colorCode))
                {
                    Serilog.Formatting.Json.JsonValueFormatter.WriteQuotedJsonString((element.Key.Value ?? "NULL").ToString(), builder);
                }

                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, ": ");

                Format(logEvent, element.Value, builder, null);
            }

            ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, "}");
        }
    }
}