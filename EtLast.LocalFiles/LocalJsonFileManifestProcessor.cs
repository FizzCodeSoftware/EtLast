﻿namespace FizzCode.EtLast;

public class LocalJsonFileManifestProcessor : IManifestProcessor
{
    public required string Directory { get; init; }
    public required Func<ContextManifest, string> FileNameGenerator { get; init; }
    public int BufferTimeoutMilliseconds { get; init; } = 2000;
    public JsonSerializerOptions SerializerOptions { get; init; } = new()
    {
        WriteIndented = true,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    private Stopwatch _lastSave;
    private IEtlContext _context;

    public void RegisterToManifestEvents(IEtlContext context, ContextManifest manifest)
    {
        _context = context;
        manifest.ManifestChanged += ManifestChanged;
        manifest.ManifestClosed += ManifestClosed;
    }

    private void ManifestClosed(ContextManifest manifest)
    {
        var ramUse = manifest.RamUse = GC.GetTotalMemory(true);
        if (ramUse > manifest.RamUsePeak)
            manifest.RamUsePeak = ramUse;

        SaveManifest(manifest);
    }

    private void ManifestChanged(ContextManifest manifest)
    {
        if (_lastSave == null || _lastSave.ElapsedMilliseconds > BufferTimeoutMilliseconds)
        {
            var ramUse = manifest.RamUse = GC.GetTotalMemory(false);
            if (ramUse > manifest.RamUsePeak)
                manifest.RamUsePeak = ramUse;

            SaveManifest(manifest);
        }
    }

    private void SaveManifest(ContextManifest manifest)
    {
        if (_lastSave == null)
            _lastSave = Stopwatch.StartNew();
        else
            _lastSave.Restart();

        if (!System.IO.Directory.Exists(Directory))
            System.IO.Directory.CreateDirectory(Directory);

        var fileName = Path.Combine(Directory, FileNameGenerator.Invoke(manifest));
        var content = JsonSerializer.Serialize(manifest, SerializerOptions);
        try
        {
            File.WriteAllText(fileName, content, Encoding.UTF8);
        }
        catch (Exception ex)
        {
            _context.Log(LogSeverity.Warning, null, "error while writing manifest: {ErrorMessage}", ex.FormatWithEtlDetails(false));
        }
    }
}