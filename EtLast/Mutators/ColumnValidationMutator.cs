namespace FizzCode.EtLast
{
    using System;
    using System.Collections.Generic;

    public class ColumnValidationMutator : AbstractMutator
    {
        public string Column { get; set; }
        public Func<IRow, bool> ErrorIf { get; set; }

        public ColumnValidationMutator(ITopic topic, string name)
            : base(topic, name)
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
                });
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