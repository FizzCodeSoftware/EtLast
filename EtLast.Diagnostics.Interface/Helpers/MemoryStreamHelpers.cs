namespace FizzCode.EtLast.Diagnostics.Interface;

using System.IO;

public static class MemoryStreamHelpers
{
    public static byte[] ReadFrom(this MemoryStream stream, long position, int size)
    {
        var oldPosition = stream.Position;
        stream.Position = position;
        var result = new byte[size];
        stream.Read(result, 0, size);
        stream.Position = oldPosition;
        return result;
    }
}
