namespace FizzCode.EtLast
{
    using System;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Xml;

    public class DataContractXmlSerializerOperation<T> : AbstractRowOperation
    {
        public RowTestDelegate If { get; set; }
        public ColumnCopyConfiguration ColumnConfiguration { get; set; }

        public InvalidValueAction ActionIfFailed { get; set; }
        public object SpecialValueIfFailed { get; set; }

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            var sourceObject = row.GetAs<T>(ColumnConfiguration.FromColumn);

            if (sourceObject == null)
                return;

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
                }
            }
            catch (Exception ex)
            {
                switch (ActionIfFailed)
                {
                    case InvalidValueAction.SetSpecialValue:
                        row.SetValue(ColumnConfiguration.ToColumn, SpecialValueIfFailed, this);
                        break;
                    case InvalidValueAction.Throw:
                        throw new OperationExecutionException(Process, this, row, "DataContract serialization failed", ex);
                    case InvalidValueAction.RemoveRow:
                        Process.RemoveRow(row, this);
                        return;
                    case InvalidValueAction.WrapError:
                        row.SetValue(ColumnConfiguration.ToColumn, new EtlRowError
                        {
                            Process = Process,
                            Operation = this,
                            OriginalValue = null,
                            Message = "DataContract serialization failed: " + ex.Message,
                        }, this);
                        break;
                }
            }
        }

        public override void Prepare()
        {
            if (ColumnConfiguration == null)
                throw new OperationParameterNullException(this, nameof(ColumnConfiguration));
        }
    }
}