namespace FizzCode.EtLast.DwhBuilder;

public interface IDwhBuilder<TTableBuilder>
    where TTableBuilder : IDwhTableBuilder
{
    string ScopeName { get; }
    IEnumerable<RelationalTable> Tables { get; }

    IReadOnlyList<SqlEngine> SupportedSqlEngines { get; }

    RelationalModel Model { get; init; }
    DwhBuilderConfiguration Configuration { get; init; }
    NamedConnectionString ConnectionString { get; init; }

    TTableBuilder[] AddTables(params RelationalTable[] tables);

    ResilientSqlScope Build();

    void AddPreFinalizer(Action<ResilientSqlScopeProcessBuilder> finalizers);
    void AddPostFinalizer(Action<ResilientSqlScopeProcessBuilder> finalizers);
}
