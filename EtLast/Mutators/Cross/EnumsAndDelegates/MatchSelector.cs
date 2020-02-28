namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate IRow MatchSelector(IRow leftRow, List<IRow> rightRows);
}