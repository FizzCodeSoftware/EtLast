namespace FizzCode.EtLast;

public sealed class LocalManifestLogSettings
{
    /// <summary>
    /// Default value is true.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Default value is null (unlimited file count).
    /// </summary>
    public int? MaxFileCount { get; set; }

    /// <summary>
    /// Default value is 16 MB.
    /// </summary>
    public int? MaxSizeOnDisk { get; set; } = 16 * 1024 * 1024;
}