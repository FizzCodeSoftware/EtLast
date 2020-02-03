namespace FizzCode.EtLast
{
    public interface IRowEqualityComparer
    {
        bool Equals(IRow leftRow, IRow rightRow);
    }
}