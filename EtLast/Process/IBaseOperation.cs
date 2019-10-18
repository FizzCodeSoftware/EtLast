namespace FizzCode.EtLast
{
    public interface IBaseOperation
    {
        string InstanceName { get; }
        string Name { get; }
        int Number { get; }

        IProcess Process { get; }

        void SetProcess(IProcess process);
        void SetParent(int index);

        void Prepare();
        void Shutdown();
    }
}