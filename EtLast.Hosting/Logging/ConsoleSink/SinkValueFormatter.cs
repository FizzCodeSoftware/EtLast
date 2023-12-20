namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class SinkValueFormatter
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
    public static string IdentityIntegerFormat { get; } = "D";

    public static void FormatScalarValue(LogEvent logEvent, TextWriter builder, ScalarValue value, string format, string propertyName)
    {
        switch (value.Value)
        {
            case null:
                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.NullValue, "NULL");
                break;
            case string strv:
                {
                    if (string.IsNullOrEmpty(propertyName) || !CustomColoredProperties.Map.TryGetValue(propertyName, out var colorCode))
                        colorCode = ColorCode.StringValue;

                    if (colorCode == ColorCode.Result && strv != "success" && strv != "completed" && strv != "finished")
                        colorCode = ColorCode.ResultFailed;

                    using (ColorCodeContext.StartOverridden(builder, logEvent, colorCode))
                    {
                        builder.Write(strv);
                    }
                }
                break;
            case bool bv:
                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.BooleanValue, bv ? "true" : "false");
                break;
            case char chv:
                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.ScalarValue, "\'" + chv.ToString(CultureInfo.InvariantCulture) + "\'");
                break;
            case sbyte:
            case byte:
            case short:
            case ushort:
            case int:
            case uint:
            case long:
            case ulong:
                {
                    if (string.IsNullOrEmpty(propertyName) || !CustomColoredProperties.Map.TryGetValue(propertyName, out var colorCode))
                        colorCode = ColorCode.NumberValue;

                    using (ColorCodeContext.StartOverridden(builder, logEvent, colorCode))
                    {
                        if (string.IsNullOrEmpty(format))
                        {
                            if (propertyName.EndsWith("Id") || propertyName.EndsWith("ID"))
                            {
                                value.Render(builder, IdentityIntegerFormat, CultureInfo.InvariantCulture);
                            }
                            else
                            {
                                value.Render(builder, DefaultIntegerFormat, CultureInfo.InvariantCulture);
                            }
                        }
                        else
                        {
                            value.Render(builder, format, CultureInfo.InvariantCulture);
                        }
                    }
                }
                break;
            case float:
            case double:
            case decimal:
                using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.NumberValue))
                {
                    if (string.IsNullOrEmpty(format))
                        value.Render(builder, "#,0.#", CultureInfo.InvariantCulture);
                    else
                        value.Render(builder, format, CultureInfo.InvariantCulture);

                    break;
                }
            case TimeSpan ts:
                using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.TimeSpanValue))
                {
                    if (string.IsNullOrEmpty(format))
                    {
                        if (ts.Days > 0)
                            value.Render(builder, @"d\.hh\:mm", CultureInfo.InvariantCulture);
                        else if (ts.Hours > 0)
                            value.Render(builder, @"h\:mm\:ss", CultureInfo.InvariantCulture);
                        else if (ts.Minutes > 0)
                            value.Render(builder, @"m\:ss", CultureInfo.InvariantCulture);
                        else if (ts.Seconds > 0)
                            value.Render(builder, @"s\.f", CultureInfo.InvariantCulture);
                        else
                            value.Render(builder, @"\.fff", CultureInfo.InvariantCulture);
                    }
                    else
                    {
                        value.Render(builder, format, CultureInfo.InvariantCulture);
                    }

                    break;
                }
            case DateTime:
                using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.ScalarValue))
                    value.Render(builder, "yyyy.MM.dd HH:mm:ss.fff", CultureInfo.InvariantCulture);

                break;
            case DateTimeOffset:
                using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.ScalarValue))
                    value.Render(builder, "yyyy.MM.dd HH:mm:ss.fff zzz", CultureInfo.InvariantCulture);

                break;
            default:
                using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.ScalarValue))
                    value.Render(builder, format, CultureInfo.InvariantCulture);

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