namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class MsSqlSessionBuilderFluent
{
    public static ISessionBuilder EnableMicrosoftSqlClient(this ISessionBuilder session)
    {
        DbProviderFactories.RegisterFactory("Microsoft.Data.SqlClient", SqlClientFactory.Instance);
        return session;
    }
}