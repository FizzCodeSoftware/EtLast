namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class ConsoleHostSessionBuilderExtensions
{
    public static ISessionBuilder UseRollingDevLogManifestFiles<T>(this ISessionBuilder session, int? maxFileCount, int? maxSizeOnDisk = 16 * 1024 * 1024)
    {
        var folder = Path.Combine(session.DevLogFolder, "manifest");
        CleanupManifestFolder(maxFileCount, maxSizeOnDisk, folder);

        return session.AddManifestProcessor(new ConsoleHostJsonManifestProcessor()
        {
            Folder = folder,
            FileNameFunc = manifest => manifest.CreatedOnUtc.ToString("yyyyMMdd-HHmmssfff", CultureInfo.InvariantCulture) + ".json",
        });
    }

    private static void CleanupManifestFolder(int? maxFileCount, int? maxSizeOnDisk, string folder)
    {
        if (maxFileCount == null && maxSizeOnDisk == null)
            return;

        if (!Directory.Exists(folder))
            return;

        if (maxFileCount != null)
        {
            var files = Directory.GetFiles(folder)
                .OrderBy(x => x)
                .ToArray();

            if (files.Length >= maxFileCount.Value)
            {
                foreach (var file in files.Take(files.Length - maxFileCount.Value + 1))
                    File.Delete(file);
            }
        }

        if (maxSizeOnDisk != null)
        {
            var files = Directory.GetFiles(folder)
                .OrderByDescending(x => x)
                .Select(x => (File: x, Info: new FileInfo(x)))
                .ToList();

            while (files.Count > 0)
            {
                var totalSize = files.Sum(x => x.Info.Length);
                if (totalSize < maxSizeOnDisk.Value)
                    break;

                File.Delete(files.Last().File);
                files.RemoveAt(files.Count - 1);
            }
        }
    }
}