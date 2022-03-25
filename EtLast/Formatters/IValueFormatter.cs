namespace FizzCode.EtLast;

using System;

public interface IValueFormatter
{
    string Format(object v, IFormatProvider formatProvider = null);
}
