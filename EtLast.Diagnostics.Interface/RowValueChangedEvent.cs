namespace FizzCode.EtLast.Diagnostics.Interface
{
    public class RowValueChangedEvent : AbstractEvent
    {
        public int RowUid { get; set; }

        public string Column { get; set; }
        public Argument PreviousValue { get; set; } // todo: this can be removed because the diagnostics service is responsible to track the current (=previous) values
        public Argument CurrentValue { get; set; }

        public string ProcessUid { get; set; }
        public string ProcessName { get; set; }
        public string OperationType { get; set; }
        public int? OperationNumber { get; set; }
        public string OperationName { get; set; }
    }
}