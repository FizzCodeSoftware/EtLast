namespace FizzCode.EtLast
{
    public interface IRowEqualityComparer
    {
        bool Equals(IValueCollection leftRow, IValueCollection rightRow);
    }
}