namespace FizzCode.EtLast
{
    public sealed class FluentProcessBuilder : IFluentProcessBuilder
    {
        internal FluentProcessBuilder()
        {
        }

        public IProducer Result { get; set; }

        public IProducer Build()
        {
            return Result;
        }

        public IFluentProcessMutatorBuilder ReadFrom(IProducer process)
        {
            Result = process;
            return new FluentProcessMutatorBuilder(this);
        }
    }
}