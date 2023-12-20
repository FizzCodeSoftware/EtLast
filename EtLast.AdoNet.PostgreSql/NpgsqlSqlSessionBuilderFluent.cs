namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class NpgsqlSqlSessionBuilderFluent
{
    public static ISessionBuilder EnablePostgreSqlClient(this ISessionBuilder session, bool enableLegacyTimestampBehavior)
    {
        DbProviderFactories.RegisterFactory("Npgsql", Npgsql.NpgsqlFactory.Instance);
        AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", enableLegacyTimestampBehavior);
        return session;
    }
}