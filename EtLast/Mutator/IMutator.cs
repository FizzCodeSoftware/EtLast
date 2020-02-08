namespace FizzCode.EtLast
{
    public interface IMutator : IEvaluable
    {
        public IEvaluable InputProcess { get; set; }
    }
}