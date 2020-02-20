namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;

    /// <summary>
    /// Producer processes create rows. They may create or generate, read from different sources, copy from existing rows.
    /// </summary>
    public abstract class AbstractProducerProcess : AbstractEvaluableProcess
    {
        /// <summary>
        /// Default false.
        /// </summary>
        public bool IgnoreRowsWithError { get; set; } = false;

        /// <summary>
        /// Default true.
        /// </summary>
        public bool IgnoreNullOrEmptyRows { get; set; } = true;

        /// <summary>
        /// First row index is (integer) 1
        /// </summary>
        public string AddRowIndexToColumn { get; set; }

        /// <summary>
        /// Default false.
        /// </summary>
        public bool ProduceAbandonedRows { get; set; }

        private int _currentRowIndex;
        protected bool AutomaticallyEvaluateAndYieldInputProcessRows { get; set; } = true;

        protected AbstractProducerProcess(ITopic topic, string name)
            : base(topic, name)
        {
        }

        protected sealed override IEnumerable<IRow> EvaluateImpl()
        {
            if (Context.CancellationTokenSource.IsCancellationRequested)
                yield break;

            var resultCount = 0;
            foreach (var row in Produce())
            {
                if (ProcessRowBeforeYield(row))
                {
                    resultCount++;
                    CounterCollection.IncrementCounter("produced rows", 1, true);
                    yield return row;
                }
            }

            Context.Log(LogSeverity.Debug, this, "produced and returned {RowCount} rows in {Elapsed}", resultCount, LastInvocationStarted.Elapsed);

            LogCounters();
        }

        protected abstract IEnumerable<IRow> Produce();

        private bool ProcessRowBeforeYield(IRow row)
        {
            if (IgnoreRowsWithError && row.HasError())
            {
                CounterCollection.IncrementCounter("ignored rows - error", 1, true);
                return false;
            }

            if (IgnoreNullOrEmptyRows && row.IsNullOrEmpty())
            {
                CounterCollection.IncrementCounter("ignored rows - error", 1, true);
                return false;
            }

            _currentRowIndex++;

            if (AddRowIndexToColumn != null && !row.HasValue(AddRowIndexToColumn))
                row.SetValue(AddRowIndexToColumn, _currentRowIndex);

            return true;
        }

        protected object HandleConverter(object value, ReaderDefaultColumnConfiguration configuration)
        {
            if (value == null)
            {
                return configuration.NullSourceHandler switch
                {
                    NullSourceHandler.WrapError => new EtlRowError()
                    {
                        Process = this,
                        OriginalValue = null,
                        Message = string.Format(CultureInfo.InvariantCulture, "failed to convert by {0}", configuration.Converter.GetType().GetFriendlyTypeName()),
                    },
                    NullSourceHandler.SetSpecialValue => configuration.SpecialValueIfSourceIsNull,
                    _ => throw new NotImplementedException(configuration.NullSourceHandler.ToString() + " is not supported yet"),
                };
            }

            if (value != null && configuration.Converter != null)
            {
                var newValue = configuration.Converter.Convert(value);
#pragma warning disable RCS1173 // Use coalesce expression instead of if.
                if (newValue != null)
                    return newValue;
#pragma warning restore RCS1173 // Use coalesce expression instead of if.

                return configuration.InvalidSourceHandler switch
                {
                    InvalidSourceHandler.WrapError => new EtlRowError()
                    {
                        Process = this,
                        OriginalValue = value,
                        Message = string.Format(CultureInfo.InvariantCulture, "failed to convert by {0}", configuration.Converter.GetType().GetFriendlyTypeName()),
                    },
                    InvalidSourceHandler.SetSpecialValue => configuration.SpecialValueIfSourceIsInvalid,
                    _ => throw new NotImplementedException(configuration.NullSourceHandler.ToString() + " is not supported yet"),
                };
            }

            return value;
        }
    }
}