namespace FizzCode.EtLast.Tests
{
    using System.Collections.Generic;

    public class TestExecuterResult
    {
        public IEvaluable Input { get; set; }
        public List<IMutator> Mutators { get; set; }
        public IEvaluable Process { get; set; }
        public List<ISlimRow> InputRows { get; set; }
        public List<ISlimRow> MutatedRows { get; set; }
    }
}