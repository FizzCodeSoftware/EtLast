namespace FizzCode.EtLast
{
    using System.Globalization;
    using System.Linq;

    public class ThrowExceptionOnRowErrorOperation : AbstractRowOperation
    {
        public RowTestDelegate If { get; set; }

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            if (row.HasError())
            {
                var exception = new EtlException(Process, "invalid value(s) found");

                var index = 0;
                foreach (var kvp in row.Values.Where(kvp => kvp.Value is EtlRowError))
                {
                    var error = kvp.Value as EtlRowError;
                    exception.Data.Add("Operation" + index.ToString("D", CultureInfo.InvariantCulture), error.Operation?.Name);
                    exception.Data.Add("Column" + index.ToString("D", CultureInfo.InvariantCulture), kvp.Key);
                    exception.Data.Add("Value" + index.ToString("D", CultureInfo.InvariantCulture), error.OriginalValue != null
                        ? error.OriginalValue + " (" + error.OriginalValue.GetType().GetFriendlyTypeName() + ")"
                        : "NULL");
                    index++;
                }

                exception.Data.Add("Row", row.ToDebugString());

                throw exception;
            }
        }

        protected override void PrepareImpl()
        {
        }
    }
}