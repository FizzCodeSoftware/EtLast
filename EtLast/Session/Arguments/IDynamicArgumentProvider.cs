namespace FizzCode.EtLast
{
    using System.Collections.Generic;

    public interface IDyamicArgumentProvider
    {
        public Dictionary<string, object> Arguments(Dictionary<string, object> existingArguments);
    }
}