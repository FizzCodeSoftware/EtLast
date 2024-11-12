namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class SessionBuilderFluent
{
    public static ISessionBuilder UseRollingDevLogManifestFiles(this ISessionBuilder session, int? maxFileCount = null, int? maxSizeOnDisk = 256 * 1024 * 1024)
    {
        var directory = Path.Combine(session.DevLogDirectory, "manifest");
        CleanupManifestDirectory(maxFileCount, maxSizeOnDisk, directory);

        return session.AddManifestProcessor(() => new CommandServiceJsonManifestProcessor()
        {
            Directory = directory,
            FileNameGenerator = manifest => manifest.ContextId.ToString("D", CultureInfo.InvariantCulture) + ".json",
        });
    }

    private static void CleanupManifestDirectory(int? maxFileCount, int? maxSizeOnDisk, string directory)
    {
        if (maxFileCount == null && maxSizeOnDisk == null)
            return;

        if (!Directory.Exists(directory))
            return;

        if (maxFileCount != null)
        {
            var files = Directory.GetFiles(directory)
                .Order()
                .ToArray();

            if (files.Length >= maxFileCount.Value)
            {
                foreach (var file in files.Take(files.Length - maxFileCount.Value + 1))
                    File.Delete(file);
            }
        }

        if (maxSizeOnDisk != null)
        {
            var files = Directory.GetFiles(directory)
                .OrderByDescending(x => x)
                .Select(x => (File: x, Info: new FileInfo(x)))
                .ToList();

            while (files.Count > 0)
            {
                var totalSize = files.Sum(x => x.Info.Length);
                if (totalSize < maxSizeOnDisk.Value)
                    break;

                File.Delete(files[^1].File);
                files.RemoveAt(files.Count - 1);
            }
        }
    }
}