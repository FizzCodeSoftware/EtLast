namespace FizzCode.EtLast.DwhBuilder.Alpha
{
    public delegate IEvaluable InputProcessCreatorDelegate(DwhTableBuilder tableBuilder);

    public static class InputIsCustomProcessExtension
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