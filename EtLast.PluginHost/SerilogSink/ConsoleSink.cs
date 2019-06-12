namespace FizzCode.EtLast.PluginHost.SerilogSink
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using Serilog.Core;
    using Serilog.Events;
    using Serilog.Formatting.Display;
    using Serilog.Parsing;

    internal class ConsoleSink : ILogEventSink
    {
        private readonly List<Action<LogEvent, TextWriter>> _writers;
        private readonly object _lock = new object();

        public ConsoleSink(string outputTemplate)
        {
            var template = new MessageTemplateParser().Parse(outputTemplate);

            var defaultValueFormatter = new DefaultValueFormatter();
            AbstractValueFormatter jsonValueFormatter = new JsonValueFormatter(defaultValueFormatter);

            _writers = new List<Action<LogEvent, TextWriter>>();
            foreach (var token in template.Tokens)
            {
                switch (token)
                {
                    case TextToken textToken:
                        _writers.Add((e, b) => WriteText(e, b, textToken.Text));
                        break;
                    case PropertyToken propertyToken:
                        switch (propertyToken.PropertyName)
                        {
                            case OutputProperties.LevelPropertyName:
                                _writers.Add(WriteLevel);
                                break;
                            case OutputProperties.NewLinePropertyName:
                                _writers.Add((_, b) => WriteNewLine(b));
                                break;
                            case OutputProperties.ExceptionPropertyName:
                                _writers.Add(WriteException);
                                break;
                            case OutputProperties.MessagePropertyName:
                                {
                                    var isLiteral = propertyToken.Format?.Any(x => x == 'l') == true;
                                    var valueFormatter = propertyToken.Format?.Any(x => x == 'j') == true ? jsonValueFormatter : defaultValueFormatter;
                                    _writers.Add((e, b) => WriteMessage(e, b, valueFormatter, isLiteral));
                                    break;
                                }
                            case OutputProperties.TimestampPropertyName:
                                _writers.Add((e, b) => WriteTimeStamp(e, b, propertyToken.Format));
                                break;
                            case "Properties":
                                {
                                    var valueFormatter = propertyToken.Format?.Any(x => x == 'j') == true ? jsonValueFormatter : defaultValueFormatter;
                                    _writers.Add((e, b) => WriteProperties(e, b, template, valueFormatter));
                                    break;
                                }
                            default:
                                _writers.Add((e, b) => WriteProperty(e, b, propertyToken.PropertyName, propertyToken.Format));
                                break;
                        }
                        break;
                }
            }
        }

        public void Emit(LogEvent logEvent)
        {
            if (logEvent == null)
                return;

            var builder = new StringWriter(new StringBuilder(1024));
            foreach (var writer in _writers)
            {
                writer.Invoke(logEvent, builder);
            }

            lock (_lock)
            {
                Console.Out.Write(builder.ToString());
                Console.Out.Flush();
            }
        }

        private static void WriteText(LogEvent logEvent, TextWriter builder, string text)
        {
            ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, text);
        }

        private static void WriteLevel(LogEvent logEvent, TextWriter builder)
        {
            var text = GetLevelNameAbbreviation();
            var colorCode = GetLevelColorCode();

            ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Value, "[");
            ColorCodeContext.Write(builder, colorCode, text);

            string GetLevelNameAbbreviation()
            {
                switch (logEvent.Level)
                {
                    case LogEventLevel.Verbose:
                        return "VRB";
                    case LogEventLevel.Debug:
                        return "DBG";
                    case LogEventLevel.Information:
                        return "INF";
                    case LogEventLevel.Warning:
                        return "WRN";
                    case LogEventLevel.Error:
                        return "ERR";
                    case LogEventLevel.Fatal:
                        return "FTL";
                    default:
                        return null;
                }
            }

            ColorCode GetLevelColorCode()
            {
                switch (logEvent.Level)
                {
                    case LogEventLevel.Verbose:
                        return ColorCode.LvlTokenVrb;
                    case LogEventLevel.Debug:
                        return ColorCode.LvlTokenDbg;
                    case LogEventLevel.Information:
                        return ColorCode.LvlTokenInf;
                    case LogEventLevel.Warning:
                        return ColorCode.LvlTokenWrn;
                    case LogEventLevel.Error:
                        return ColorCode.LvlTokenErr;
                    case LogEventLevel.Fatal:
                        return ColorCode.LvlTokenFtl;
                    default:
                        return ColorCode.LvlTokenInf;
                }
            }
        }

        private static void WriteNewLine(TextWriter builder)
        {
            builder.WriteLine();
        }

        private static void WriteException(LogEvent logEvent, TextWriter builder)
        {
            if (logEvent.Exception == null)
                return;

            var lines = logEvent.Exception.ToString().Split(new string[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var line in lines)
            {
                var colorCode = line.StartsWith("   ")
                    ? ColorCode.TimeStamp_Property_Exception
                    : ColorCode.Message_Exception;

                ColorCodeContext.Write(builder, colorCode, line + Environment.NewLine);
            }
        }

        private static void WriteMessage(LogEvent logEvent, TextWriter builder, AbstractValueFormatter valueFormatter, bool topLevelScalar)
        {
            foreach (var token in logEvent.MessageTemplate.Tokens)
            {
                switch (token)
                {
                    case TextToken tt:
                        {
                            ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.Message_Exception, tt.Text);
                            break;
                        }
                    case PropertyToken pt:
                        {
                            if (!logEvent.Properties.TryGetValue(pt.PropertyName, out var value))
                            {
                                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.TimeStamp_Property_Exception, pt.ToString());
                            }
                            else if (topLevelScalar && value is ScalarValue sv && sv.Value is string text)
                            {
                                ColorCodeContext.WriteOverridden(builder, logEvent, ColorCode.StringValue, text);
                            }
                            else
                            {
                                valueFormatter.Format(logEvent, value, builder, pt.Format, topLevelScalar);
                            }

                            break;
                        }
                }
            }
        }

        private static void WriteTimeStamp(LogEvent logEvent, TextWriter builder, string format)
        {
            using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.TimeStamp_Property_Exception))
            {
                new ScalarValue(logEvent.Timestamp).Render(builder, format);
            }
        }

        private static void WriteProperties(LogEvent logEvent, TextWriter builder, MessageTemplate outputTemplate, AbstractValueFormatter valueFormatter)
        {
            var properties = new List<LogEventProperty>();
            foreach (var kvp in logEvent.Properties)
            {
                if (!logEvent.MessageTemplate.Tokens.Any(t => t is PropertyToken pt && pt.PropertyName == kvp.Key) && !outputTemplate.Tokens.Any(t => t is PropertyToken pt && pt.PropertyName == kvp.Key))
                {
                    properties.Add(new LogEventProperty(kvp.Key, kvp.Value));
                }
            }

            valueFormatter.FormatStructureValue(logEvent, builder, new StructureValue(properties), null);
        }

        private static void WriteProperty(LogEvent logEvent, TextWriter builder, string propertyName, string format)
        {
            if (!logEvent.Properties.TryGetValue(propertyName, out var propertyValue))
                return;

            using (ColorCodeContext.StartOverridden(builder, logEvent, ColorCode.TimeStamp_Property_Exception))
            {
                propertyValue.Render(builder, format);
            }
        }
    }
}