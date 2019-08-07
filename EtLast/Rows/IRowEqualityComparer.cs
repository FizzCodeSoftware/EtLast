namespace FizzCode.EtLast
{
    public interface IRowEqualityComparer
    {
        bool Compare(IRow leftRow, IRow rightRow);
    }
}