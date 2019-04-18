﻿namespace FizzCode.EtLast
{
    public class ThrowExceptionOnRowErrorOperation : AbstractRowOperation
    {
        public IfDelegate If { get; set; }

        public override void Apply(IRow row)
        {
            var result = If == null || If.Invoke(row);
            if (result != true) return;

            if (row.HasError())
            {
                var exception = new InvalidValueException(Process, row);
                throw exception;
            }
        }

        public override void Prepare()
        {
        }
    }
}