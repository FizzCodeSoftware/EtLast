namespace FizzCode.EtLast.PluginHost.Excellence
{
    using System.Collections.Generic;
    using System.Globalization;
    using FizzCode.EtLast;
    using FizzCode.EtLast.EPPlus;

    public class ContactReadWrite : AbstractContactPlugin
    {
        public override void Execute()
        {
            Context.ExecuteOne(true, new BasicScope(Context, null, null)
            {
                ProcessCreator = ProcessCreator,
            });
        }

        private IEnumerable<IExecutable> ProcessCreator(IExecutable scope)
        {
            yield return new DeleteFileProcess(Context, "DeleteFile", scope.Topic)
            {
                FileName = OutputFileName,
            };

            yield return new MutatorBuilder()
            {
                InputProcess = new EpPlusExcelReaderProcess(Context, "Reader", scope.Topic)
                {
                    FileName = SourceFileName,
                    SheetName = "People",
                    ColumnConfiguration = new List<ReaderColumnConfiguration>()
                    {
                        new ReaderColumnConfiguration("Name", new StringConverter(formatProviderHint: CultureInfo.InvariantCulture)),
                        new ReaderColumnConfiguration("Age", new IntConverterAuto(formatProviderHint: CultureInfo.InvariantCulture)),
                    },
                },
                Mutators = new List<IMutator>()
                {
                    new EpPlusSimpleRowWriterMutator(Context, "Writer", scope.Topic)
                    {
                        FileName = OutputFileName,
                        SheetName = "output",
                        ColumnConfiguration = new List<ColumnCopyConfiguration>()
                        {
                            new ColumnCopyConfiguration("Name", "Contact name"),
                            new ColumnCopyConfiguration("Age", "Contact age"),
                        },
                        Finalize = (package, state) => state.LastWorksheet.Cells.AutoFitColumns(),
                    }
                },
            }.BuildEvaluable();
        }
    }
}