namespace FizzCode.EtLast
{
    public interface IJob
    {
        string Name { get; }
        string InstanceName { get; set; }

        int Number { get; }
        IfJobDelegate If { get; }
        void Execute();

        IProcess Process { get; }
        public void SetProcess(IProcess process, int number);
    }
}