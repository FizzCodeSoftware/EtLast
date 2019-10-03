namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate IRow MatchingRowSelector(IRow leftRow, Dictionary<string, IRow> lookup);
}