namespace FizzCode.EtLast.DwhBuilder.MsSql;

using System;

public delegate IProducer CustomInputProcessCreatorDelegate(DwhTableBuilder tableBuilder, DateTimeOffset? maxRecordTimestamp);

public static partial class TableBuilderExtensions
{
    public static DwhTableBuilder[] InputIsCustomProcess(this DwhTableBuilder[] builders, CustomInputProcessCreatorDelegate inputProcessCreator)
    {
        foreach (var builder in builders)
        {
            builder.SetInputProcessCreator(maxRecordTimestamp => inputProcessCreator.Invoke(builder, maxRecordTimestamp));
        }

        return builders;
    }
}
