namespace FizzCode.EtLast
{
    using System;

    public class OperationProcessConfiguration
    {
        public int WorkerCount { get; set; } = Math.Max(1, MachineCpuCoreCount.Value - 1);
        public Type WorkerType { get; set; } = typeof(DefaultInProcessWorker);
        public Type RowQueueType { get; set; } = typeof(DefaultRowQueue);
        public bool KeepOrder { get; set; } = false;

        public int InputBufferSize { get; set; } = 250;

        public int MainLoopDelay { get; set; } = 250;

        public int ThrottlingLimit { get; set; } = 1000;
        public int ThrottlingMaxSleep { get; set; } = 1000;

        public int ThrottlingSleepResolution { get; set; } = 10;

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