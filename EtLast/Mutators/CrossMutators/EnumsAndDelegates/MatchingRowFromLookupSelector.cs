namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate IRow MatchingRowFromLookupSelector(IRow leftRow, Dictionary<string, List<IRow>> lookup);
}