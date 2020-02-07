namespace FizzCode.EtLast
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Xml;

    public class DataContractXmlDeSerializerOperation<T> : AbstractRowOperation
    {
        public RowTestDelegate If { get; set; }
        public ColumnCopyConfiguration ColumnConfiguration { get; set; }

        public InvalidValueAction ActionIfFailed { get; set; }
        public object SpecialValueIfFailed { get; set; }

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            var sourceByteArray = row.GetAs<byte[]>(ColumnConfiguration.FromColumn);

            if (sourceByteArray == null)
                return;

            var startedOn = Stopwatch.StartNew();

            try
            {
                using (var ms = new MemoryStream(sourceByteArray))
                {
                    object obj = null;
                    using (var reader = XmlDictionaryReader.CreateTextReader(sourceByteArray, XmlDictionaryReaderQuotas.Max))
                    {
                        var ser = new DataContractSerializer(typeof(T));
                        obj = ser.ReadObject(reader, true);
                    }

                    row.SetValue(ColumnConfiguration.ToColumn, obj, this);

                    var time = startedOn.Elapsed;

                    CounterCollection.IncrementTimeSpan("deserialization time (DataContract XML)", time);
                    CounterCollection.IncrementCounter("deserialization count (DataContract XML)", 1);

                    CounterCollection.IncrementTimeSpan("deserialization time (DataContract XML) - " + TypeHelpers.GetFriendlyTypeName(typeof(T)), time);
                    CounterCollection.IncrementCounter("deserialization count (DataContract XML) - " + TypeHelpers.GetFriendlyTypeName(typeof(T)), 1);
                }
            }
            catch (Exception ex)
            {
                var time = startedOn.Elapsed;
                CounterCollection.IncrementTimeSpan("deserialization time (DataContract XML) - error", time);
                CounterCollection.IncrementCounter("deserialization count (DataContract XML) - error", 1);

                CounterCollection.IncrementTimeSpan("deserialization time (DataContract XML) - error - " + TypeHelpers.GetFriendlyTypeName(typeof(T)), time);
                CounterCollection.IncrementCounter("deserialization count (DataContract XML) - error - " + TypeHelpers.GetFriendlyTypeName(typeof(T)), 1);

                switch (ActionIfFailed)
                {
                    case InvalidValueAction.SetSpecialValue:
                        row.SetValue(ColumnConfiguration.ToColumn, SpecialValueIfFailed, this);
                        break;
                    case InvalidValueAction.Throw:
                        throw new OperationExecutionException(Process, this, row, "DataContract XML deserialization failed", ex);
                    case InvalidValueAction.RemoveRow:
                        Process.RemoveRow(row, this);
                        return;
                    case InvalidValueAction.WrapError:
                        row.SetValue(ColumnConfiguration.ToColumn, new EtlRowError
                        {
                            Process = Process,
                            Operation = this,
                            OriginalValue = null,
                            Message = "DataContract XML deserialization failed: " + ex.Message,
                        }, this);
                        break;
                }
            }
        }

        protected override void PrepareImpl()
        {
            if (ColumnConfiguration == null)
                throw new OperationParameterNullException(this, nameof(ColumnConfiguration));
        }
    }
}