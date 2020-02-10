namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Xml;

    public class DataContractXmlDeSerializerMutator<T> : AbstractMutator
    {
        public ColumnCopyConfiguration ColumnConfiguration { get; set; }

        public InvalidValueAction ActionIfFailed { get; set; }
        public object SpecialValueIfFailed { get; set; }

        public DataContractXmlDeSerializerMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            var sourceByteArray = row.GetAs<byte[]>(ColumnConfiguration.FromColumn);

            if (sourceByteArray == null)
            {
                yield return row;
                yield break;
            }

            var startedOn = Stopwatch.StartNew();
            var removeRow = false;
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

                    row.SetValue(this, ColumnConfiguration.ToColumn, obj);

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
                        row.SetValue(this, ColumnConfiguration.ToColumn, SpecialValueIfFailed);
                        break;
                    case InvalidValueAction.Throw:
                        throw new ProcessExecutionException(this, row, "DataContract XML deserialization failed", ex);
                    case InvalidValueAction.RemoveRow:
                        removeRow = true;
                        break;
                    case InvalidValueAction.WrapError:
                        row.SetValue(this, ColumnConfiguration.ToColumn, new EtlRowError
                        {
                            Process = this,
                            OriginalValue = null,
                            Message = "DataContract XML deserialization failed: " + ex.Message,
                        });
                        break;
                }
            }

            if (!removeRow)
                yield return row;
        }

        protected override void ValidateMutator()
        {
            if (ColumnConfiguration == null)
                throw new ProcessParameterNullException(this, nameof(ColumnConfiguration));
        }
    }
}