﻿namespace FizzCode.EtLast
{
    using System;

    public class OrderedOperationProcessConfiguration
    {
        public Type RowQueueType { get; set; } = typeof(DefaultRowQueue);

        public int InputBufferSize { get; set; } = 250;

        public int MainLoopDelay { get; set; } = 250;

        public int ThrottlingLimit { get; set; } = 1000;
        public int ThrottlingMaxSleep { get; set; } = 1000;

        public int ThrottlingSleepResolution { get; set; } = 10;
    }
}