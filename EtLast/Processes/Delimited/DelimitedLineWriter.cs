namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Text;

    public sealed class WriteToDelimitedMutator : AbstractMutator, IRowSink
    {
        public ISinkProvider SinkProvider { get; init; }

        /// <summary>
        /// Default value is <see cref="Encoding.UTF8"/>
        /// </summary>
        public Encoding Encoding { get; init; } = Encoding.UTF8;

        /// <summary>
        /// Default value is \r\n
        /// </summary>
        public string LineEnding { get; init; } = "\r\n";

        /// <summary>
        /// Default value is "
        /// </summary>
        public char Quote { get; init; } = '\"';

        /// <summary>
        /// Default value is "
        /// </summary>
        public char Escape { get; init; } = '\"';

        /// <summary>
        /// Default value is ';'.
        /// </summary>
        public char Delimiter { get; init; } = ';';

        /// <summary>
        /// Default value is true
        /// </summary>
        public bool WriteHeader { get; init; } = true;

        /// <summary>
        /// Key is column in the row, value is column in the delimited stream (can be null).
        /// </summary>
        public Dictionary<string, string> Columns { get; init; }

        private NamedSink _sink;
        private int _rowsWritten;
        private byte[] _delimiterBytes;
        private byte[] _lineEndingBytes;
        private string _escapedQuote;
        private char[] _quoteRequiredChars;
        private string _quoteAsString;

        public WriteToDelimitedMutator(IEtlContext context)
            : base(context)
        {
        }

        protected override void ValidateMutator()
        {
            base.ValidateMutator();

            if (Columns == null)
                throw new ProcessParameterNullException(this, nameof(Columns));
        }

        protected override void StartMutator()
        {
            _sink = SinkProvider.GetSink(this);
            _delimiterBytes = Encoding.GetBytes(new[] { Delimiter });
            _lineEndingBytes = Encoding.GetBytes(LineEnding);
            _escapedQuote = new string(new[] { Escape, Quote });
            _quoteRequiredChars = new[] { Quote, Escape, '\r', '\n' };
            _quoteAsString = Quote.ToString();

            _rowsWritten = 0;
            if (WriteHeader)
            {
                var first = true;
                foreach (var kvp in Columns)
                {
                    if (!first)
                        _sink.Stream.Write(_delimiterBytes);

                    var str = kvp.Value ?? kvp.Key;
                    if (str != null)
                    {
                        var quoteRequired = !string.IsNullOrEmpty(str) &&
                            (str.IndexOfAny(_quoteRequiredChars) > -1
                            || str[0] == ' '
                            || str[^1] == ' '
                            || str.Contains(LineEnding, StringComparison.Ordinal));

                        var line = ConvertToDelimitedValue(str, quoteRequired);
                        _sink.Stream.Write(Encoding.GetBytes(line));
                    }

                    first = false;
                }

                _rowsWritten++;
            }
        }

        protected override void CloseMutator()
        {
            if (_sink != null && SinkProvider.AutomaticallyDispose)
            {
                _sink.Stream.Flush();
                _sink.Stream.Close();
                _sink.Stream.Dispose();
                _sink = null;
            }
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            Context.RegisterWriteToSink(row, _sink.SinkUid);

            try
            {
                if (_rowsWritten > 0)
                    _sink.Stream.Write(_lineEndingBytes);

                _rowsWritten++;

                var first = true;
                foreach (var kvp in Columns)
                {
                    if (!first)
                        _sink.Stream.Write(_delimiterBytes);

                    var value = row[kvp.Key];
                    if (value != null)
                    {
                        var str = DefaultValueFormatter.Format(value);
                        var quoteRequired = !string.IsNullOrEmpty(str) &&
                            (str.IndexOfAny(_quoteRequiredChars) > -1
                            || str[0] == ' '
                            || str[^1] == ' '
                            || str.Contains(LineEnding, StringComparison.Ordinal));

                        var line = ConvertToDelimitedValue(str, quoteRequired);
                        _sink.Stream.Write(Encoding.GetBytes(line));
                    }

                    first = false;
                }
            }
            catch (Exception ex)
            {
                Context.RegisterIoCommandFailed(this, _sink.IoCommandKind, _sink.IoCommandUid, _rowsWritten, ex);
                throw;
            }

            yield return row;
        }

        private string ConvertToDelimitedValue(string value, bool quoteRequired)
        {
            if (quoteRequired)
            {
                if (value != null)
                {
                    value = value.Replace(_quoteAsString, _escapedQuote, StringComparison.Ordinal);
                }

                value = Quote + value + Quote;
            }

            return value;
        }
    }

    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public static class WriteToDelimitedMutatorFluent
    {
        /// <summary>
        /// Write rows to a delimited stream.
        /// </summary>
        public static IFluentProcessMutatorBuilder WriteToDelimited(this IFluentProcessMutatorBuilder builder, WriteToDelimitedMutator mutator)
        {
            return builder.AddMutator(mutator);
        }
    }
}