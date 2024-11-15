﻿namespace FizzCode.EtLast;

internal struct ColorCodeContext(TextWriter builder) : IDisposable
{
    private readonly TextWriter _builder = builder;
    private static readonly IDictionary<ColorCode, string> _colorCodeValues = new Dictionary<ColorCode, string>
    {
        //[ColorCode.Exception] = "\x1b[38;5;0015m",
        [ColorCode.TimeStamp_Property_Exception] = "\x1b[38;5;7m",
        [ColorCode.Value] = "\x1b[38;5;8m",
        [ColorCode.NullValue] = "\x1b[38;5;27m",
        [ColorCode.StructureName] = "\x1b[38;5;7m",
        [ColorCode.StringValue] = "\x1b[38;5;45m",
        [ColorCode.NumberValue] = "\x1b[38;5;204m",
        [ColorCode.BooleanValue] = "\x1b[38;5;33m",
        [ColorCode.ScalarValue] = "\x1b[38;5;85m",
        [ColorCode.TimeSpanValue] = "\x1b[38;5;220m",
        [ColorCode.LvlTokenVrb] = "\x1b[38;5;237m",
        [ColorCode.LvlTokenDbg] = "\x1b[38;5;8m",
        [ColorCode.LvlTokenInf] = "\x1b[38;5;15m",
        [ColorCode.LvlTokenWrn] = "\x1b[38;5;0m\x1b[48;5;214m",
        [ColorCode.LvlTokenErr] = "\x1b[38;5;197m",
        [ColorCode.LvlTokenFtl] = "\x1b[38;5;15m\x1b[48;5;196m",
        //[ColorCode.Module] = "\x1b[38;5;0007m",
        //[ColorCode.Plugin] = "\x1b[38;5;0007m",
        [ColorCode.UNUSED] = "\x1b[38;5;133m",
        [ColorCode.Process] = "\x1b[38;5;228m",
        [ColorCode.Task] = "\x1b[38;5;209m",
        [ColorCode.Operation] = "\x1b[38;5;85m",
        [ColorCode.Job] = "\x1b[38;5;85m",
        [ColorCode.ConnectionStringName] = "\x1b[38;5;135m",
        [ColorCode.Location] = "\x1b[38;5;35m",
        [ColorCode.Transaction] = "\x1b[38;5;245m",
        [ColorCode.Result] = "\x1b[38;5;15m",
        [ColorCode.ResultFailed] = "\x1b[38;5;15m\x1b[48;5;196m",
    };

    private const string ResetColorCodeValue = "\x1b[0m";

#pragma warning disable IDE0251 // Make member 'readonly'
    public void Dispose()
#pragma warning restore IDE0251 // Make member 'readonly'
    {
        _builder.Write(ResetColorCodeValue);
    }

    internal static ColorCodeContext StartOverridden(TextWriter builder, LogEvent logEvent, ColorCode colorCode)
    {
        colorCode = GetOverridenColorCode(logEvent.Level, colorCode);
        if (_colorCodeValues.TryGetValue(colorCode, out var colorCodeValue))
        {
            builder.Write(colorCodeValue);
        }

        return new ColorCodeContext(builder);
    }

    internal static void Write(TextWriter builder, ColorCode colorCode, string text)
    {
        if (_colorCodeValues.TryGetValue(colorCode, out var value))
        {
            builder.Write(value);
        }

        builder.Write(text);
        builder.Write(ResetColorCodeValue);
    }

    internal static void WriteOverridden(TextWriter builder, LogEvent logEvent, ColorCode colorCode, string text)
    {
        colorCode = GetOverridenColorCode(logEvent.Level, colorCode);
        if (_colorCodeValues.TryGetValue(colorCode, out var colorCodeValue))
        {
            builder.Write(colorCodeValue);
        }

        builder.Write(text);
        builder.Write(ResetColorCodeValue);
    }

    internal static ColorCode GetOverridenColorCode(LogEventLevel level, ColorCode colorCode)
    {
        return level switch
        {
            LogEventLevel.Verbose => ColorCode.LvlTokenVrb,
            LogEventLevel.Debug => ColorCode.LvlTokenDbg,
            LogEventLevel.Warning => ColorCode.LvlTokenWrn,
            LogEventLevel.Error => ColorCode.LvlTokenErr,
            LogEventLevel.Fatal => ColorCode.LvlTokenFtl,
            _ => colorCode,
        };
    }
}
