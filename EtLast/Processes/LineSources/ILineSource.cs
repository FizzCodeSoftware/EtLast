namespace FizzCode.EtLast
{
    public interface ILineSource
    {
        public string GetTopic();
        public void Prepare(IProcess caller);
        public string ReadLine(IProcess caller);
        public void Release(IProcess caller);
        public int GetIoCommandUid();
    }
}