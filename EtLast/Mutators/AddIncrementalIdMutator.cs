namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public class AddIncrementalIdMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        public string Column { get; set; }

        /// <summary>
        /// Default value is 0.
        /// </summary>
        public int FirstId { get; set; }

        public AddIncrementalIdMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            var nextId = FirstId;

            var rows = InputProcess.Evaluate().TakeRowsAndTransferOwnership(this);
            foreach (var row in rows)
            {
                row.SetValue(Column, nextId, this);

                yield return row;
                nextId++;
            }
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            if (string.IsNullOrEmpty(Column))
                throw new ProcessParameterNullException(this, nameof(Column));
        }
    }
}