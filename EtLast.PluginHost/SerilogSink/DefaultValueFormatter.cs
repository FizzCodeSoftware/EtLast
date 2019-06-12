namespace FizzCode.EtLast.PluginHost.SerilogSink
{
    using System.IO;
    using Serilog.Events;

    internal class DefaultValueFormatter : AbstractValueFormatter
    {
        public override void FormatScalarValue(LogEvent logEvent, TextWriter builder, ScalarValue value, string format, bool topLevelScalar)
        {
            switch (value.Value)
            {
                case null:
                    ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.NullValue, "NULL");
                    break;
                case string strv:
                    using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.StringValue))
                    {
                        if (format != "l")
                        {
                            Serilog.Formatting.Json.JsonValueFormatter.WriteQuotedJsonString(strv, builder);
                        }
                        else
                        {
                            builder.Write(strv);
                        }

                        break;
                    }
                case bool bv:
                    ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.BooleanValue, bv ? "true" : "false");
                    break;
                case char chv:
                    ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.ScalarValue, "\'" + chv + "\'");
                    break;
                case sbyte _:
                case byte _:
                case short _:
                case ushort _:
                case int _:
                case uint _:
                case long _:
                case ulong _:
                case float _:
                case double _:
                case decimal _:
                    using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.NumberValue))
                    {
                        value.Render(builder, format);
                        break;
                    }
                default:
                    {
                        using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.ScalarValue))
                        {
                            value.Render(builder, format);
                        }

                        break;
                    }
            }
        }

        public override void FormatStructureValue(LogEvent logEvent, TextWriter builder, StructureValue value, string format)
        {
            if (value.TypeTag != null)
            {
                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.StructureName, value.TypeTag + " ");
            }

            ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, "{");

            var isFirst = true;
            foreach (var property in value.Properties)
            {
                if (!isFirst)
                    ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, ", ");

                isFirst = false;

                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.StructureName, property.Name);
                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, "=");

                Format(logEvent, property.Value, builder, null);
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

                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, "[");

                using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.StringValue))
                {
                    Format(logEvent, element.Key, builder, null);
                }

                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, "]=");

                Format(logEvent, element.Value, builder, null);
            }

            ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, "}");
        }
    }
}