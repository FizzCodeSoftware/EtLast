namespace FizzCode.EtLast.Diagnostics.Interface
{
    public class IoCommandEndEvent : IoCommandEvent
    {
        public int AffectedDataCount { get; set; }
        public string ErrorMessage { get; set; }
    }
}