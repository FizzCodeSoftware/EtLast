﻿namespace FizzCode.EtLast
{
    using System;

    public class ColumnValidationOperation : AbstractRowOperation
    {
        public IfDelegate If { get; set; }
        public string Column { get; set; }
        public Func<IRow, bool> ErrorIf { get; set; }

        public override void Apply(IRow row)
        {
            var result = If?.Invoke(row) != false;
            if (!result) return;

            Stat.IncrementCounter("executed", 1);

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

        public override void Prepare()
        {
            if (string.IsNullOrEmpty(Column)) throw new OperationParameterNullException(this, nameof(Column));
            if (ErrorIf == null) throw new OperationParameterNullException(this, nameof(ErrorIf));
        }
    }
}