namespace FizzCode.EtLast
{
    public interface ISlimRow : IReadOnlySlimRow, IEditableRow
    {
        new object this[string column] { get; set; }
        new object Tag { get; set; }
    }
}