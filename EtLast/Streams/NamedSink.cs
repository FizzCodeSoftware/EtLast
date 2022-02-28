namespace FizzCode.EtLast
{
    using System.IO;

    public class NamedSink : NamedStream
    {
        public int SinkUid { get; }
        public int RowsWritten { get; private set; }

        public NamedSink(string name, Stream stream, int ioCommandUid, IoCommandKind ioCommandKind, int sinkUid)
            : base(name, stream, ioCommandUid, ioCommandKind)
        {
            SinkUid = sinkUid;
        }

        public void IncreaseRowsWritten()
        {
            RowsWritten++;
        }
    }
}