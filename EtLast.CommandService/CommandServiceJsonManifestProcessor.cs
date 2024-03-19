namespace FizzCode.EtLast;

internal class CommandServiceJsonManifestProcessor : IManifestProcessor
{
    public required string Directory { get; init; }
    public required Func<ContextManifest, string> FileNameGenerator { get; init; }
    public int BufferTimeoutMilliseconds { get; init; } = 2000;
    public JsonSerializerOptions JsonSerializerOptions { get; init; } = new()
    {
        WriteIndented = true,
    };

    private Stopwatch _lastSave = null;
    private IEtlContext _context;

    public void RegisterToManifestEvents(IEtlContext context, ContextManifest manifest)
    {
        _context = context;
        manifest.ManifestChanged += ManifestChanged;
        manifest.ManifestClosed += ManifestClosed;
    }

    private void ManifestClosed(ContextManifest manifest)
    {
        SaveManifest(manifest);
    }

    private void ManifestChanged(ContextManifest manifest)
    {
        if (_lastSave == null || _lastSave.ElapsedMilliseconds > BufferTimeoutMilliseconds)
            SaveManifest(manifest);
    }

    private void SaveManifest(ContextManifest manifest)
    {
        if (_lastSave == null)
            _lastSave = Stopwatch.StartNew();
        else
            _lastSave.Restart();

        if (!System.IO.Directory.Exists(Directory))
            System.IO.Directory.CreateDirectory(Directory);

        var path = Path.Combine(Directory, FileNameGenerator.Invoke(manifest));
        var content = JsonSerializer.Serialize(manifest, JsonSerializerOptions);
        try
        {
            File.WriteAllText(path, content, Encoding.UTF8);
        }
        catch (Exception ex)
        {
            _context.Log(LogSeverity.Warning, null, "error while writing manifest: {ErrorMessage}", ex.FormatExceptionWithDetails(false));
        }
    }
}