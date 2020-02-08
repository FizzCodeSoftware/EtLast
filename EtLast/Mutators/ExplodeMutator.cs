namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate IEnumerable<IRow> ExplodeDelegate(ExplodeMutator process, IRow row);

    public class ExplodeMutator : AbstractEvaluableProcess, IMutator
    {
        public IEvaluable InputProcess { get; set; }

        /// <summary>
        /// Default true.
        /// </summary>
        public bool RemoveOriginalRow { get; set; } = true;

        public ExplodeDelegate RowCreator { get; set; }

        public ExplodeMutator(IEtlContext context, string name, string topic)
            : base(context, name, topic)
        {
        }

        protected override IEnumerable<IRow> EvaluateImpl()
        {
            var rows = InputProcess.Evaluate().TakeRowsAndTransferOwnership(this);

            foreach (var row in rows)
            {
                var newRows = RowCreator.Invoke(this, row);

                if (RemoveOriginalRow)
                {
                    Context.SetRowOwner(row, null);
                }
                else
                {
                    yield return row;
                }

                if (newRows != null)
                {
                    foreach (var newRow in newRows)
                    {
                        yield return newRow;
                    }
                }
            }
        }

        protected override void ValidateImpl()
        {
            if (InputProcess == null)
                throw new ProcessParameterNullException(this, nameof(InputProcess));

            if (RowCreator == null)
                throw new ProcessParameterNullException(this, nameof(RowCreator));
        }
    }
}