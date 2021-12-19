namespace FizzCode.EtLast
{
    using System.IO;

    public class NamedStream
    {
        public string Name { get; }
        public Stream Stream { get; private set; }

        public NamedStream(string name, Stream stream)
        {
            Name = name;
            Stream = stream;
        }

        public void Dispose()
        {
            if (Stream != null)
            {
                Stream.Dispose();
                Stream = null;
            }
        }
    }
}