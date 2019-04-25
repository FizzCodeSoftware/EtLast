namespace FizzCode.EtLast
{
    using System.Threading;

    public class AddIncrementalIdSequentialOperation : AbstractRowOperation
    {
        public string Column { get; set; }
        public int FirstId { get; set; } = 0;

        private int _nextId;

        public override void Apply(IRow row)
        {
            var id = Interlocked.Increment(ref _nextId) - 1;
            row.SetValue(Column, id, this);
        }

        public override void Prepare()
        {
            _nextId = FirstId;
        }
    }
}