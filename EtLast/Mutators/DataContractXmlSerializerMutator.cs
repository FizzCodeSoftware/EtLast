namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Xml;

    public class DataContractXmlSerializerMutator<T> : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public RowTestDelegate If { get; set; }
        public ColumnCopyConfiguration ColumnConfiguration { get; set; }

        public InvalidValueAction ActionIfFailed { get; set; }
        public object SpecialValueIfFailed { get; set; }

        public DataContractXmlSerializerMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            var rows = InputProcess.Evaluate().TakeRowsAndTransferOwnership(this);

            foreach (var row in rows)
            {
                if (If?.Invoke(row) == false)
                {
                    yield return row;
                    continue;
                }

                var sourceObject = row.GetAs<T>(ColumnConfiguration.FromColumn);
                if (sourceObject == null)
                {
                    yield return row;
                    continue;
                }

                var startedOn = Stopwatch.StartNew();
                var removeRow = false;
                try
                {
                    using (var ms = new MemoryStream())
                    {
                        using (var writer = XmlDictionaryWriter.CreateTextWriter(ms))
                        {
                            var ser = new DataContractSerializer(typeof(T));
                            ser.WriteObject(writer, sourceObject);
                        }

                        var data = ms.ToArray();
                        row.SetValue(ColumnConfiguration.ToColumn, data, this);

                        var time = startedOn.Elapsed;

                        CounterCollection.IncrementTimeSpan("serialization time (DataContract XML)", time);
                        CounterCollection.IncrementCounter("serialization count (DataContract XML)", 1);

                        CounterCollection.IncrementTimeSpan("serialization time (DataContract XML) - " + TypeHelpers.GetFriendlyTypeName(typeof(T)), time);
                        CounterCollection.IncrementCounter("serialization count (DataContract XML) - " + TypeHelpers.GetFriendlyTypeName(typeof(T)), 1);
                    }
                }
                catch (Exception ex)
                {
                    var time = startedOn.Elapsed;
                    CounterCollection.IncrementTimeSpan("serialization time (DataContract XML) - error", time);
                    CounterCollection.IncrementCounter("serialization count (DataContract XML) - error", 1);

                    CounterCollection.IncrementTimeSpan("serialization time (DataContract XML) - error - " + TypeHelpers.GetFriendlyTypeName(typeof(T)), time);
                    CounterCollection.IncrementCounter("serialization count (DataContract XML) - error - " + TypeHelpers.GetFriendlyTypeName(typeof(T)), 1);

                    switch (ActionIfFailed)
                    {
                        case InvalidValueAction.SetSpecialValue:
                            row.SetValue(ColumnConfiguration.ToColumn, SpecialValueIfFailed, this);
                            break;
                        case InvalidValueAction.Throw:
                            throw new ProcessExecutionException(this, row, "DataContract XML serialization failed", ex);
                        case InvalidValueAction.RemoveRow:
                            removeRow = true;
                            break;
                        case InvalidValueAction.WrapError:
                            row.SetValue(ColumnConfiguration.ToColumn, new EtlRowError
                            {
                                Process = this,
                                OriginalValue = null,
                                Message = "DataContract XML serialization failed: " + ex.Message,
                            }, this);
                            break;
                    }
                }

                if (removeRow)
                {
                    Context.SetRowOwner(row, null);
                }
                else
                {
                    yield return row;
                }
            }
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));
        }
    }
}