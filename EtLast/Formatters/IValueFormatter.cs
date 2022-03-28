namespace FizzCode.EtLast;

public interface IValueFormatter
{
    string Format(object v, IFormatProvider formatProvider = null);
}
