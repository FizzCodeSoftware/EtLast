namespace FizzCode.EtLast
{
    using System;

    public class OperationProcessConfiguration : BasicOperationProcessConfiguration
    {
        public int WorkerCount { get; set; } = Math.Max(1, MachineCpuCoreCount.Value - 1);
        public Type WorkerType { get; set; } = typeof(DefaultInProcessWorker);
        public bool KeepOrder { get; set; } = false;

        public static Lazy<int> MachineCpuCoreCount = new Lazy<int>(() =>
        {
            var coreCount = 0;
            try
            {
                foreach (var item in new System.Management.ManagementObjectSearcher("Select * from Win32_Processor").Get())
                {
                    coreCount += int.Parse(item["NumberOfCores"].ToString());
                }
            }
            catch (Exception)
            {
                coreCount = Environment.ProcessorCount;
            }

            return coreCount;
        }, true);
    }
}