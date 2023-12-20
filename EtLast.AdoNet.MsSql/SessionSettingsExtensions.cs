namespace FizzCode.EtLast;

public static class SessionSettingsExtensions
{
    public static T UseSqlClient<T>(this T settings) where T: SessionSettings
    {
        DbProviderFactories.RegisterFactory("Microsoft.Data.SqlClient", SqlClientFactory.Instance);
        return settings;
    }
}