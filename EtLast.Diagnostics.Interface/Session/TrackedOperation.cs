namespace FizzCode.EtLast.Diagnostics.Interface
{
    using System.Diagnostics;
    using System.Globalization;

    [DebuggerDisplay("{ToDisplayValue()}")]
    public class TrackedOperation
    {
        public int Uid { get; }
        public string Type { get; }
        public string InstanceName { get; }
        public TrackedProcess Process { get; }
        public string DisplayName { get; }

        public TrackedOperation(int uid, string type, string instanceName, TrackedProcess process)
        {
            Uid = uid;
            Type = type;
            InstanceName = instanceName;
            Process = process;
            DisplayName = Uid.ToString("D2", CultureInfo.InvariantCulture) + "."
                + (InstanceName != null
                    ? InstanceName + " (" + Type + ")"
                    : Type);
        }
    }
}