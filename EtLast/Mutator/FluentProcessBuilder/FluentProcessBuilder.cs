namespace FizzCode.EtLast
{
    public sealed class FluentProcessBuilder : IFluentProcessBuilder
    {
        internal FluentProcessBuilder()
        {
        }

        public IEvaluable Result { get; set; }

        public IEvaluable Build()
        {
            return Result;
        }

        public IFluentProcessMutatorBuilder ReadFrom(IEvaluable process)
        {
            Result = process;
            return new FluentProcessMutatorBuilder(this);
        }
    }
}