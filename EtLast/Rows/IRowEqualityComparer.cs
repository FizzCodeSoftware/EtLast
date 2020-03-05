namespace FizzCode.EtLast
{
    public interface IRowEqualityComparer
    {
        bool Equals(IReadOnlyRow leftRow, IReadOnlyRow rightRow);
    }
}