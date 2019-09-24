namespace FizzCode.EtLast.PluginHost.SerilogSink
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Serilog.Events;

    internal struct ColorCodeContext : IDisposable
    {
#pragma warning disable IDE0069 // Disposable fields should be disposed
        private readonly TextWriter _builder;
#pragma warning restore IDE0069 // Disposable fields should be disposed

        private static readonly IDictionary<ColorCode, string> _colorCodeValues = new Dictionary<ColorCode, string>
        {
            [ColorCode.Message_Exception] = "\x1b[38;5;0015m",
            [ColorCode.TimeStamp_Property_Exception] = "\x1b[38;5;0007m",
            [ColorCode.Value] = "\x1b[38;5;0008m",
            [ColorCode.NullValue] = "\x1b[38;5;0027m",
            [ColorCode.StructureName] = "\x1b[38;5;0007m",
            [ColorCode.StringValue] = "\x1b[38;5;0045m",
            [ColorCode.NumberValue] = "\x1b[38;5;0200m",
            [ColorCode.BooleanValue] = "\x1b[38;5;0027m",
            [ColorCode.ScalarValue] = "\x1b[38;5;0085m",
            [ColorCode.LvlTokenVrb] = "\x1b[38;5;0007m",
            [ColorCode.LvlTokenDbg] = "\x1b[38;5;0007m",
            [ColorCode.LvlTokenInf] = "\x1b[38;5;0015m",
            [ColorCode.LvlTokenWrn] = "\x1b[38;5;000m\x1b[48;5;11m",
            [ColorCode.LvlTokenErr] = "\x1b[38;5;0015m\x1b[48;5;0196m",
            [ColorCode.LvlTokenFtl] = "\x1b[38;5;0015m\x1b[48;5;0196m",
        };

        public ColorCodeContext(TextWriter builder)
        {
            _builder = builder;
        }

        public void Dispose()
        {
            _builder.Write("\x1b[0m");
        }

        internal static ColorCodeContext StartOverridden(TextWriter builder, LogEvent logEvent, ColorCode colorCode)
        {
            colorCode = GetOverridenColorCode(logEvent, colorCode);

            if (_colorCodeValues.TryGetValue(colorCode, out var value))
            {
                builder.Write(value);
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
            builder.Write("\x1b[0m");
        }

        internal static void WriteOverridden(TextWriter builder, LogEvent logEvent, ColorCode colorCode, string text)
        {
            colorCode = GetOverridenColorCode(logEvent, colorCode);
            if (_colorCodeValues.TryGetValue(colorCode, out var value))
            {
                builder.Write(value);
            }

            builder.Write(text);
            builder.Write("\x1b[0m");
        }

        internal static ColorCode GetOverridenColorCode(LogEvent logEvent, ColorCode colorCode)
        {
            return logEvent.Level switch
            {
                LogEventLevel.Warning => ColorCode.LvlTokenWrn,
                LogEventLevel.Fatal => ColorCode.LvlTokenFtl,
                LogEventLevel.Error => ColorCode.LvlTokenErr,
                _ => colorCode,
            };
        }
    }
}