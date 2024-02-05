namespace FizzCode.EtLast;

public interface IManifestProcessor
{
    public void RegisterToManifestEvents(IEtlContext context, ContextManifest manifest);
}
