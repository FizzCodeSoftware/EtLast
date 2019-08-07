namespace FizzCode.EtLast.Rows
{
    public interface IRowEqualityComparer
    {
        bool Compare(IRow leftRow, IRow rightRow);
    }
}