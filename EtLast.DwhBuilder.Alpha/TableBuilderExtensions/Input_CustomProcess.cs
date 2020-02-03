namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    public delegate IEvaluable InputProcessCreatorDelegate(DwhTableBuilder tableBuilder);

    public static partial class TableBuilderExtensions
    {
        public static DwhTableBuilder[] Input_CustomProcess(this DwhTableBuilder[] builders, InputProcessCreatorDelegate inputProcessCreator)
        {
            foreach (var builder in builders)
            {
                builder.SetInputProcessCreator(() => inputProcessCreator.Invoke(builder));
            }

            return builders;
        }
    }
}