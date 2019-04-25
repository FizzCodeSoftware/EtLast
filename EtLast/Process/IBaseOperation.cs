namespace FizzCode.EtLast
{
    public interface IBaseOperation
    {
        string InstanceName { get; }
        string Name { get; }
        int Index { get; }

        IProcess Process { get; }
        void SetParent(IProcess process, int index);

        void Prepare();
        void Shutdown();
    }
}