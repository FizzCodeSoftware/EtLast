namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class ColumnValidationMutator : AbstractMutator
    {
        public string Column { get; set; }
        public Func<IRow, bool> ErrorIf { get; set; }

        protected ColumnValidationMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> MutateRow(IRow row)
        {
            if (ErrorIf(row))
            {
                row.SetValue(Column, new EtlRowError()
                {
                    Process = this,
                    OriginalValue = row[Column],
                    Message = "validation failed",
                }, this);
            }

            yield return row;
        }

        protected override void ValidateMutator()
        {
            if (string.IsNullOrEmpty(Column))
                throw new ProcessParameterNullException(this, nameof(Column));

            if (ErrorIf == null)
                throw new ProcessParameterNullException(this, nameof(ErrorIf));
        }
    }
}