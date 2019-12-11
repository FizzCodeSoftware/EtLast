namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate IEnumerable<IRow> ExplodeDelegate(IRowOperation operation, IRow row);

    public class ExplodeOperation : AbstractRowOperation
    {
        /// <summary>
        /// Default true.
        /// </summary>
        public bool RemoveOriginalRow { get; set; } = true;

        public RowTestDelegate If { get; set; }
        public ExplodeDelegate Then { get; set; }
        public ExplodeDelegate Else { get; set; }

        private readonly List<IRow> _buffer = new List<IRow>();

        public override void Apply(IRow row)
        {
            IEnumerable<IRow> newRows = null;

            if (If != null)
            {
                var result = If.Invoke(row);
                if (result)
                {
                    newRows = Then.Invoke(this, row);
                    CounterCollection.IncrementDebugCounter("then executed", 1);
                }
                else if (Else != null)
                {
                    newRows = Else.Invoke(this, row);
                    CounterCollection.IncrementDebugCounter("else executed", 1);
                }
            }
            else
            {
                newRows = Then.Invoke(this, row);
                CounterCollection.IncrementDebugCounter("then executed", 1);
            }

            if (RemoveOriginalRow)
            {
                Process.RemoveRow(row, this);
            }

            if (newRows != null)
            {
                foreach (var newRow in newRows)
                {
                    newRow.CurrentOperation = this;
                    _buffer.Add(newRow);
                }

                Process.AddRows(_buffer, this);
                _buffer.Clear();
            }
        }

        public override void Prepare()
        {
            if (Then == null)
                throw new OperationParameterNullException(this, nameof(Then));

            if (Else != null && If == null)
                throw new OperationParameterNullException(this, nameof(If));
        }
    }
}