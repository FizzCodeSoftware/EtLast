namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;

    public sealed class DbTableDefinition
    {
        public string TableName { get; init; }

        /// <summary>
        /// Key is name in row, value is name in database table.
        /// </summary>
        public Dictionary<string, string> Columns { get; init; }
    }
}