namespace FizzCode.EtLast.Tests.Integration.Modules.DwhBuilderTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FizzCode.EtLast;
    using FizzCode.EtLast.AdoNet;
    using FizzCode.LightWeight.AdoNet;

    public abstract class AbstractDwhBuilderTestFlow : AbstractEtlFlow
    {
        protected DateTime EtlRunId1 { get; } = new DateTime(2001, 1, 1, 1, 1, 1, DateTimeKind.Utc);
        protected DateTime EtlRunId2 { get; } = new DateTime(2022, 2, 2, 2, 2, 2, DateTimeKind.Utc);

        protected List<ISlimRow> ReadRows(IProcess caller, NamedConnectionString connectionString, string schema, string table)
        {
            return new AdoNetDbReader(Context)
            {
                Name = "Reader",
                ConnectionString = connectionString,
                TableName = connectionString.Escape(table, schema),
            }.Evaluate(caller).TakeRowsAndReleaseOwnership().ToList();
        }
    }
}