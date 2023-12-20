namespace FizzCode.EtLast;

[EditorBrowsable( EditorBrowsableState.Never)]
public static class MsSqlSessionBuilderExtensions
{
    public static ISessionBuilder EnableSqlClient(this ISessionBuilder session)
    {
        DbProviderFactories.RegisterFactory("Microsoft.Data.SqlClient", SqlClientFactory.Instance);
        return session;
    }
}