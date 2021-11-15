namespace FizzCode.EtLast.DwhBuilder
{
    using System.Collections.Generic;
    using FizzCode.EtLast.AdoNet;
    using FizzCode.LightWeight.AdoNet;
    using FizzCode.LightWeight.RelationalModel;

    public interface IDwhBuilder<TTableBuilder>
        where TTableBuilder : IDwhTableBuilder
    {
        string ScopeName { get; }
        IEnumerable<RelationalTable> Tables { get; }
        string Topic { get; }

        IReadOnlyList<SqlEngine> SupportedSqlEngines { get; }

        RelationalModel Model { get; init; }
        DwhBuilderConfiguration Configuration { get; init; }
        NamedConnectionString ConnectionString { get; init; }

        TTableBuilder[] AddTables(params RelationalTable[] tables);
        void AddPostFinalizer(ResilientSqlScopeExecutableCreatorDelegate creator);

        ResilientSqlScope Build();
    }
}