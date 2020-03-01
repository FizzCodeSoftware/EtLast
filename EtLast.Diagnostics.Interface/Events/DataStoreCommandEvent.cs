namespace FizzCode.EtLast.Diagnostics.Interface
{
    public abstract class DataStoreCommandEvent : AbstractEvent
    {
        public int Uid { get; set; }
    }
}