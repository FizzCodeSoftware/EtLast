namespace FizzCode.EtLast.PluginHost.Excellence
{
    using System.IO;
    using FizzCode.EtLast;

    public abstract class AbstractContactPlugin : AbstractEtlPlugin
    {
        public string SourceFileName => Path.Combine(GetStorageFolder(), "contacts.xlsx");
        public string OutputFileName => Path.Combine(GetStorageFolder("output"), GetType().Name + ".xlsx");
    }
}