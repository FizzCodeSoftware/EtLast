namespace FizzCode.EtLast
{
    using System.Threading;

    public class AddIncrementalIdOperation : AbstractRowOperation
    {
        public string Column { get; set; }

        /// <summary>
        /// Default value is 0.
        /// </summary>
        public int FirstId { get; set; }

        private int _nextId;

        public override void Apply(IRow row)
        {
            var id = Interlocked.Increment(ref _nextId) - 1;
            row.SetValue(Column, id, this);
        }

        protected override void PrepareImpl()
        {
            _nextId = FirstId;
        }
    }
}