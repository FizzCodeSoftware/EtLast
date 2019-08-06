namespace FizzCode.EtLast
{
    using System;

    public class OperationProcessConfiguration
    {
        public Type RowQueueType { get; set; } = typeof(DefaultRowQueue);

        public int InputBufferSize { get; set; } = 250;

        public int MainLoopDelay { get; set; } = 250;

        public int ThrottlingLimit { get; set; } = 50000;
        public int ThrottlingMaxSleep { get; set; } = 1000;

        public int ThrottlingSleepResolution { get; set; } = 10;

        public bool KeepOrder { get; set; }
    }
}