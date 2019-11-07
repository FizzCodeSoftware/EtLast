namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;

    /// <summary>
    /// Producer processes create rows. They may create or generate, read from different sources, copy from existing rows.
    /// </summary>
    public abstract class AbstractProducerProcess : AbstractEvaluableProcess
    {
        public bool IgnoreRowsWithError { get; set; } = false;

        /// <summary>
        /// The process evaluates and yields the rows from the input process.
        /// </summary>
        public IEvaluable InputProcess { get; set; }

        /// <summary>
        /// First row index is (integer) 1
        /// </summary>
        public string AddRowIndexToColumn { get; set; }

        private int _currentRowIndex;

        protected AbstractProducerProcess(IEtlContext context, string name = null)
            : base(context, name)
        {
        }

        protected sealed override IEnumerable<IRow> Evaluate(Stopwatch startedOn)
        {
            if (InputProcess != null)
            {
                Context.Log(LogSeverity.Information, this, "evaluating <{InputProcess}>", InputProcess.Name);

                var fetchedRowCount = 0;
                var returnedRowCount = 0;
                var inputRows = InputProcess.Evaluate(this);
                foreach (var row in inputRows)
                {
                    fetchedRowCount++;
                    if (ProcessRowBeforeYield(row))
                    {
                        returnedRowCount++;
                        yield return row;
                    }
                }

                Context.Log(LogSeverity.Debug, this, "fetched {FetchedRowCount} and returned {ReturnedRowCount} rows from {InputProcess} in {Elapsed}",
                    fetchedRowCount, returnedRowCount, InputProcess.Name, startedOn.Elapsed);
            }

            var resultCount = 0;
            foreach (var row in Produce(startedOn))
            {
                if (ProcessRowBeforeYield(row))
                {
                    resultCount++;
                    yield return row;
                }
            }

            Context.Log(LogSeverity.Debug, this, "produced and returned {RowCount} rows in {Elapsed}", resultCount, startedOn.Elapsed);
        }

        protected abstract IEnumerable<IRow> Produce(Stopwatch startedOn);

        private bool ProcessRowBeforeYield(IRow row)
        {
            if (IgnoreRowsWithError && row.HasError())
                return false;

            _currentRowIndex++;

            if (AddRowIndexToColumn != null && !row.Exists(AddRowIndexToColumn))
                row.SetValue(AddRowIndexToColumn, _currentRowIndex, this);

            return true;
        }

        protected static object HandleConverter(IProcess process, object value, string rowColumn, ReaderDefaultColumnConfiguration configuration, IRow row, out bool failed)
        {
            failed = false;

            if (value == null)
            {
                switch (configuration.NullSourceHandler)
                {
                    case NullSourceHandler.WrapError:
                        row.SetValue(rowColumn, new EtlRowError()
                        {
                            Process = process,
                            Operation = null,
                            OriginalValue = null,
                            Message = string.Format(CultureInfo.InvariantCulture, "failed to convert by {0}", TypeHelpers.GetFriendlyTypeName(configuration.Converter.GetType())),
                        }, process);
                        failed = true;
                        return value;
                    case NullSourceHandler.SetSpecialValue:
                        return configuration.SpecialValueIfSourceIsNull;
                    case NullSourceHandler.Throw:
                        throw new InvalidValueException(process, row, rowColumn);
                    default:
                        throw new NotImplementedException(configuration.NullSourceHandler.ToString() + " is not supported yet");
                }
            }

            if (value != null && configuration.Converter != null)
            {
                var newValue = configuration.Converter.Convert(value);
                if (newValue != null)
                    return newValue;

                switch (configuration.InvalidSourceHandler)
                {
                    case InvalidSourceHandler.WrapError:
                        row.SetValue(rowColumn, new EtlRowError()
                        {
                            Process = process,
                            Operation = null,
                            OriginalValue = value,
                            Message = string.Format(CultureInfo.InvariantCulture, "failed to convert by {0}", TypeHelpers.GetFriendlyTypeName(configuration.Converter.GetType())),
                        }, process);
                        break;
                    case InvalidSourceHandler.SetSpecialValue:
                        row.SetValue(rowColumn, configuration.SpecialValueIfSourceIsInvalid, process);
                        break;
                    case InvalidSourceHandler.Throw:
                        throw new InvalidValueException(process, row, rowColumn);
                    default:
                        throw new NotImplementedException(configuration.NullSourceHandler.ToString() + " is not supported yet");
                }

                failed = true;
                return value;
            }

            return value;
        }
    }
}