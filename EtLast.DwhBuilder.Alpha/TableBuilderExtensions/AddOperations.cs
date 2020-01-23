namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    using System;
    using System.Collections.Generic;

    public static class AddOperationsExtension
    {
        public static DwhTableBuilder[] AddOperations(this DwhTableBuilder[] builders, Func<DwhTableBuilder, IEnumerable<IRowOperation>> creator)
        {
            foreach (var builder in builders)
            {
                builder.AddOperationCreator(creator);
            }

            return builders;
        }
    }
}