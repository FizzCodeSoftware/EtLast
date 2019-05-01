namespace FizzCode.EtLast.Tests.Base
{
    using FizzCode.EtLast;

    /// <summary>
    /// Used to test for <see cref="EtlRowError"/>s.
    /// </summary>
    public class EtlRowErrorTest : EtlRowError
    {
        public EtlRowErrorTest(object originalValue)
        {
            OriginalValue = originalValue;
        }
    }
}
