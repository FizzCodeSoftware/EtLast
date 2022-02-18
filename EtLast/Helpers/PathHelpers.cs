namespace FizzCode.EtLast
{
    using System;
    using System.IO;
    using System.Reflection;

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
    }
}