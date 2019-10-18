namespace FizzCode.EtLast
{
    using System.Globalization;
    using System.Threading;

    public abstract class AbstractJob : IJob
    {
        public string Name { get; private set; }
        public string InstanceName { get; set; }

        public int Number { get; private set; }
        public IfJobDelegate If { get; set; }
        public IProcess Process { get; private set; }

        protected AbstractJob()
        {
            Name = "??." + TypeHelpers.GetFriendlyTypeName(GetType());
        }

        public abstract void Execute(CancellationTokenSource cancellationTokenSource);

        public void SetProcess(IProcess process, int number)
        {
            Process = process;
            Number = number;
            Name = Number.ToString("D2", CultureInfo.InvariantCulture) + "." + (InstanceName ?? TypeHelpers.GetFriendlyTypeName(GetType()));
        }
    }
}