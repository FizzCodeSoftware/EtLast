namespace FizzCode.EtLast
{
    public interface IOperation
    {
        int UID { get; }
        string InstanceName { get; }
        string Name { get; }
        int Number { get; }

        IProcess Process { get; }

        void SetProcess(IProcess process);
        void SetNumber(int index);

        void Prepare();
        void Shutdown();
    }
}