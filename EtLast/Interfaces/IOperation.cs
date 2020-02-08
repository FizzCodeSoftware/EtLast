namespace FizzCode.EtLast
{
    public interface IOperation
    {
        int UID { get; }
        string InstanceName { get; }
        string Name { get; }

        IProcess Process { get; }

        void SetProcess(IProcess process);

        void Prepare();
        void Shutdown();
    }
}