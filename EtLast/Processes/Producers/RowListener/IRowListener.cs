namespace FizzCode.EtLast.Processes.Producers.RowListener;

public interface IRowListener
{
    public void AddRow(IReadOnlySlimRow row);
}