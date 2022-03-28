namespace FizzCode.EtLast.ConsoleHost.SerilogSink;

internal static class ValueFormatter
{
    public static void Format(LogEvent logEvent, LogEventPropertyValue value, TextWriter builder, string format, string propertyName)
    {
        switch (value)
        {
            case ScalarValue sv:
                FormatScalarValue(logEvent, builder, sv, format, propertyName);
                break;
            case SequenceValue seqv:
                FormatSequenceValue(logEvent, builder, seqv, propertyName);
                break;
            case StructureValue strv:
                FormatStructureValue(logEvent, builder, strv);
                break;
            case DictionaryValue dictv:
                FormatDictionaryValue(logEvent, builder, dictv, propertyName);
                break;
            default:
                throw new NotSupportedException($"The value {value} is not of a type supported by this visitor.");
        }
    }

    private static void FormatSequenceValue(LogEvent logEvent, TextWriter builder, SequenceValue sequence, string propertyName)
    {
        ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, "[");

        var isFirst = true;
        foreach (var element in sequence.Elements)
        {
            if (!isFirst)
                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, ", ");

            isFirst = false;
            Format(logEvent, element, builder, null, propertyName);
        }

        ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, "]");
    }

    public static string DefaultIntegerFormat { get; } = "#,0";

    public static void FormatScalarValue(LogEvent logEvent, TextWriter builder, ScalarValue value, string format, string propertyName)
    {
        switch (value.Value)
        {
            case null:
                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.NullValue, "NULL");
                break;
            case string strv:
                if (string.IsNullOrEmpty(propertyName) || !CustomColoredProperties.Map.TryGetValue(propertyName, out var colorCode))
                    colorCode = ColorCode.StringValue;

                using (ColorCodeContext.StartOverridden(builder, logEvent, colorCode))
                {
                    builder.Write(strv);
                    break;
                }
            case bool bv:
                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.BooleanValue, bv ? "true" : "false");
                break;
            case char chv:
                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.ScalarValue, "\'" + chv.ToString(CultureInfo.InvariantCulture) + "\'");
                break;
            case sbyte _:
            case byte _:
            case short _:
            case ushort _:
            case int _:
            case uint _:
            case long _:
            case ulong _:
                using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.NumberValue))
                {
                    if (string.IsNullOrEmpty(format))
                    {
                        value.Render(builder, DefaultIntegerFormat, CultureInfo.InvariantCulture);
                    }
                    else
                    {
                        value.Render(builder, format, CultureInfo.InvariantCulture);
                    }

                    break;
                }
            case float _:
            case double _:
            case decimal _:
                using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.NumberValue))
                {
                    if (string.IsNullOrEmpty(format))
                    {
                        value.Render(builder, "#,0.#", CultureInfo.InvariantCulture);
                    }
                    else
                    {
                        value.Render(builder, format, CultureInfo.InvariantCulture);
                    }

                    break;
                }
            case TimeSpan ts:
                using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.TimeSpanValue))
                {
                    if (string.IsNullOrEmpty(format))
                    {
                        if (ts.Days > 0)
                        {
                            value.Render(builder, @"d\.hh\:mm", CultureInfo.InvariantCulture);
                        }
                        else if (ts.Hours > 0)
                        {
                            value.Render(builder, @"h\:mm\:ss", CultureInfo.InvariantCulture);
                        }
                        else if (ts.Minutes > 0)
                        {
                            value.Render(builder, @"m\:ss", CultureInfo.InvariantCulture);
                        }
                        else if (ts.Seconds > 0)
                        {
                            value.Render(builder, @"s\.f", CultureInfo.InvariantCulture);
                        }
                        else
                        {
                            value.Render(builder, @"\.fff", CultureInfo.InvariantCulture);
                        }
                    }
                    else
                    {
                        value.Render(builder, format, CultureInfo.InvariantCulture);
                    }

                    break;
                }
            case DateTime _:
                using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.ScalarValue))
                {
                    value.Render(builder, "yyyy.MM.dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
                }

                break;
            case DateTimeOffset _:
                using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.ScalarValue))
                {
                    value.Render(builder, "yyyy.MM.dd HH:mm:ss.fff zzz", CultureInfo.InvariantCulture);
                }

                break;
            default:
                using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.ScalarValue))
                {
                    value.Render(builder, format, CultureInfo.InvariantCulture);
                }

                break;
        }
    }

    public static void FormatStructureValue(LogEvent logEvent, TextWriter builder, StructureValue value)
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

            Format(logEvent, property.Value, builder, null, property.Name);
        }

        ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, "}");
    }

    public static void FormatDictionaryValue(LogEvent logEvent, TextWriter builder, DictionaryValue value, string propertyName)
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
                Format(logEvent, element.Key, builder, null, propertyName);
            }

            ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, "]=");

            Format(logEvent, element.Value, builder, null, propertyName);
        }

        ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, "}");
    }
}
