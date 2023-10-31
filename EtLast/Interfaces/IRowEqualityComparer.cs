namespace FizzCode.EtLast;

public interface IRowEqualityComparer
{
    bool Equals(IReadOnlySlimRow leftRow, IReadOnlySlimRow rightRow);
}
