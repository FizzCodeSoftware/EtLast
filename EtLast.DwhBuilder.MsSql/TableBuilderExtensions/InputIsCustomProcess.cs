namespace FizzCode.EtLast.DwhBuilder.MsSql
{
    public delegate IEvaluable InputProcessCreatorDelegate(DwhTableBuilder tableBuilder);

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] InputIsCustomProcess(this DwhTableBuilder[] builders, InputProcessCreatorDelegate inputProcessCreator)
        {
            foreach (var builder in builders)
            {
                builder.SetInputProcessCreator(() => inputProcessCreator.Invoke(builder));
            }

            return builders;
        }
    }
}