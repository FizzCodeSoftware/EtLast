namespace FizzCode.EtLast.Diagnostics.Interface
{
    public class DataStoreCommandEndEvent : DataStoreCommandEvent
    {
        public int AffectedDataCount { get; set; }
        public string ErrorMessage { get; set; }
    }
}