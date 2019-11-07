namespace FizzCode.EtLast
{
    public interface IProcess
    {
        IEtlContext Context { get; }
        string Name { get; }
        IProcess Caller { get; }

        void Validate();
    }
}