namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public delegate IEnumerable<IRow> EvaluateDelegate(IProcess caller);

    public interface IProcess
    {
        string Name { get; }
        IEtlContext Context { get; }
        IProcess Caller { get; }

        IEnumerable<IRow> Evaluate(IProcess caller = null);
    }
}