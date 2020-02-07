namespace FizzCode.EtLast
{
    using System;

    public class ColumnValidationOperation : AbstractRowOperation
    {
        public RowTestDelegate If { get; set; }
        public string Column { get; set; }
        public Func<IRow, bool> ErrorIf { get; set; }

        public override void Apply(IRow row)
        {
            if (If?.Invoke(row) == false)
                return;

            CounterCollection.IncrementCounter("executed", 1);

            if (ErrorIf(row))
            {
                row.SetValue(Column, new EtlRowError()
                {
                    Process = Process,
                    Operation = this,
                    OriginalValue = row[Column],
                    Message = "validation failed",
                }, this);
            }
        }

        protected override void PrepareImpl()
        {
            if (string.IsNullOrEmpty(Column))
                throw new OperationParameterNullException(this, nameof(Column));

            if (ErrorIf == null)
                throw new OperationParameterNullException(this, nameof(ErrorIf));
        }
    }
}