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
            Context.ExecuteOne(true, new BasicScope(PluginTopic)
            {
                ProcessCreator = ProcessCreator,
            });
        }

        private IEnumerable<IExecutable> ProcessCreator(BasicScope scope)
        {
            yield return new DeleteFileProcess(scope.Topic, "DeleteFile")
            {
                FileName = OutputFileName,
            };

            yield return new ProcessBuilder()
            {
                InputProcess = new EpPlusExcelReaderProcess(scope.Topic, "Reader")
                {
                    FileName = SourceFileName,
                    SheetName = "People",
                    ColumnConfiguration = new List<ReaderColumnConfiguration>()
                    {
                        new ReaderColumnConfiguration("Name", new StringConverter(formatProviderHint: CultureInfo.InvariantCulture)),
                        new ReaderColumnConfiguration("Age", new IntConverterAuto(formatProviderHint: CultureInfo.InvariantCulture)),
                    },
                },
                Mutators = new MutatorList()
                {
                    new EpPlusSimpleRowWriterMutator(scope.Topic, "Writer")
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
            }.Build();
        }
    }
}