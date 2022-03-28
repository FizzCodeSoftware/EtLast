using System.Reflection;

namespace FizzCode.EtLast;

public static class PathHelpers
{
    private static readonly Lazy<string> BaseFolder = new(() =>
    {
        var folder = Path.GetDirectoryName((Assembly.GetEntryAssembly() ?? Assembly.GetCallingAssembly()).Location);
        if (!folder.EndsWith(Path.DirectorySeparatorChar))
            folder += Path.DirectorySeparatorChar;

        return folder;
    });

    public static string GetFriendlyPathName(string path)
    {
        if (string.IsNullOrEmpty(path))
            return null;

        if (!path.StartsWith(".", StringComparison.InvariantCultureIgnoreCase) && !path.StartsWith(Path.DirectorySeparatorChar))
        {
            try
            {
                var relPath = Path.GetRelativePath(BaseFolder.Value, path);
                if (relPath.Length < path.Length)
                    return relPath;

                return path;
            }
            catch (Exception)
            {
            }
        }

        return path;
    }

    public static string CombineUrl(params string[] parts)
    {
        if (parts == null || parts.Length == 0)
            return null;

        if (parts.Length == 1)
            return parts[0];

        var sb = new StringBuilder();

        var endsWithSlash = false;
        for (var i = 0; i < parts.Length; i++)
        {
            var part = parts[i].Trim();
            if (i > 0)
                part = part.TrimStart('/');

            if (string.IsNullOrEmpty(part))
                continue;

            if (i > 0 && !endsWithSlash)
                sb.Append('/');

            sb.Append(part);

            endsWithSlash = part.EndsWith('/');
        }

        return sb.ToString();
    }
}
