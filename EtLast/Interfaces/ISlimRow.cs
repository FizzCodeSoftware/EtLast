namespace FizzCode.EtLast
{
    public interface ISlimRow : IReadOnlySlimRow, IEditableRow
    {
        new object Tag { get; set; }
    }
}