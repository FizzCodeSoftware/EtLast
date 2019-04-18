namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public abstract class AbstractBaseProducerProcess : IProcess
    {
        public IEtlContext Context { get; }
        public string Name { get; }
        public IProcess Caller { get; protected set; }
        public IProcess InputProcess { get; set; }

        protected AbstractBaseProducerProcess(IEtlContext context, string name = null)
        {
            Context = context ?? throw new InvalidProcessParameterException(this, nameof(context), context, InvalidOperationParameterException.ValueCannotBeNullMessage);
            Name = name;
        }

        public abstract IEnumerable<IRow> Evaluate(IProcess caller = null);
    }
}