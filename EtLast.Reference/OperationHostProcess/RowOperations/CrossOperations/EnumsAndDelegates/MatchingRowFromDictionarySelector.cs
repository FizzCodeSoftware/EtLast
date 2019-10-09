namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate IRow MatchingRowFromDictionarySelector(IRow leftRow, Dictionary<string, IRow> dictionary);
}