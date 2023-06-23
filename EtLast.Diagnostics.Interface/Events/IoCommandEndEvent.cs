namespace FizzCode.EtLast.Diagnostics.Interface;

public class IoCommandEndEvent : IoCommandEvent
{
    public long? AffectedDataCount { get; set; }
    public string ErrorMessage { get; set; }
}
