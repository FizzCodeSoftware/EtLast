namespace FizzCode.EtLast.Tests
{
    using System.Collections.Generic;

    public class TestExecuterResult
    {
        public IProducer Process { get; set; }
        public List<ISlimRow> MutatedRows { get; set; }
    }
}