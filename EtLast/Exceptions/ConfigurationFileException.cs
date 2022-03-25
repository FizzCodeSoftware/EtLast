namespace FizzCode.EtLast;

using System;
using System.Runtime.InteropServices;

[ComVisible(true)]
[Serializable]
public class ConfigurationFileException : EtlException
{
    public ConfigurationFileException(string path, string message)
        : base(message)
    {
        Data.Add("Path", path);
    }

    public ConfigurationFileException(string path, string message, Exception innerException)
        : base(message, innerException)
    {
        Data.Add("Path", path);
    }
}
