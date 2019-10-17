namespace FizzCode.EtLast.PluginHost.Excellence
{
    using System.IO;
    using FizzCode.EtLast;

    public abstract class AbstractContactPlugin : AbstractEtlPlugin
    {
        public string SourceFileName => Path.Combine(GetStorageFolder("Excellence"), "contacts.xlsx");
        public string OutputFileName => Path.Combine(GetStorageFolder("Excellence", "output"), GetType().Name + ".xlsx");
    }
}