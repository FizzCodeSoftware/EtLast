namespace FizzCode.EtLast.AdoNet
{
    using System.Collections.Generic;

    public sealed class DbTableDefinition
    {
        public string TableName { get; init; }

        /// <summary>
        /// Key is column in the row, value is column in the database table (can be null).
        /// </summary>
        public Dictionary<string, string> Columns { get; init; }
    }
}