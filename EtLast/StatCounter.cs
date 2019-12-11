namespace FizzCode.EtLast
{
    public class StatCounter
    {
        public string Name { get; internal set; }
        public string Code { get; internal set; }
        public long Value { get; internal set; }
        public bool IsDebug { get; internal set; }

        public StatCounter Clone()
        {
            return new StatCounter()
            {
                Name = Name,
                Code = Code,
                Value = Value,
                IsDebug = IsDebug,
            };
        }
    }
}