namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate IEnumerable<IRow> EvaluateDelegate(IExecutionBlock caller);

    public interface IProcess : IExecutionBlock
    {
        IEtlContext Context { get; }
        new string Name { get; set; }

        /// <summary>
        /// Some consumer processes use buffering to process the rows enumerated from their input.
        /// If a process can only return data really slowly by design then it should allow the consumer to process the rows immediately by setting this value to true.
        /// </summary>
        bool ConsumerShouldNotBuffer { get; }

        IEnumerable<IRow> Evaluate(IExecutionBlock caller = null);
    }
}