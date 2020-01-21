namespace FizzCode.EtLast.Diagnostics.Interface
{
    public class RowValueChangedEvent : AbstractEvent
    {
        public int RowUid { get; set; }

        public string Column { get; set; }
        public Argument CurrentValue { get; set; }

        public int? ProcessUid { get; set; }

        public string OperationType { get; set; }
        public int? OperationNumber { get; set; }
        public string OperationName { get; set; }
    }
}