namespace FizzCode.EtLast
{
    public interface ISlimRow : IReadOnlySlimRow
    {
        new object this[string column] { get; set; }
        new object Tag { get; set; }
        void Clear();
    }
}