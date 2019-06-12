namespace FizzCode.EtLast.PluginHost.SerilogSink
{
    using System;
    using System.IO;
    using Serilog.Events;

    internal abstract class AbstractValueFormatter
    {
        public void Format(LogEvent logEvent, LogEventPropertyValue value, TextWriter builder, string format, bool topLevelScalar = false)
        {
            switch (value)
            {
                case ScalarValue sv:
                    FormatScalarValue(logEvent, builder, sv, format, topLevelScalar);
                    break;
                case SequenceValue seqv:
                    FormatSequenceValue(logEvent, builder, seqv);
                    break;
                case StructureValue strv:
                    FormatStructureValue(logEvent, builder, strv, format);
                    break;
                case DictionaryValue dictv:
                    FormatDictionaryValue(logEvent, builder, dictv, format);
                    break;
                default:
                    throw new NotSupportedException($"The value {value} is not of a type supported by this visitor.");
            }
        }

        private void FormatSequenceValue(LogEvent logEvent, TextWriter builder, SequenceValue sequence)
        {
            ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, "[");

            var isFirst = true;
            foreach (var element in sequence.Elements)
            {
                if (!isFirst)
                    ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, ", ");

                isFirst = false;
                Format(logEvent, element, builder, null);
            }

            ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, "]");
        }

        public abstract void FormatDictionaryValue(LogEvent logEvent, TextWriter builder, DictionaryValue value, string format);
        public abstract void FormatScalarValue(LogEvent logEvent, TextWriter builder, ScalarValue value, string format, bool topLevelScalar);
        public abstract void FormatStructureValue(LogEvent logEvent, TextWriter builder, StructureValue value, string format);
    }
}