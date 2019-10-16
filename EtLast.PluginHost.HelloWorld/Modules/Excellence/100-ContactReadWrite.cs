namespace FizzCode.EtLast.PluginHost.Excellence
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using FizzCode.EtLast;
    using FizzCode.EtLast.EPPlus;

    public class ContactReadWrite : AbstractContactPlugin
    {
        public override void Execute()
        {
            Context.ExecuteOne(true, new DefaultEtlStrategy(ProcessCreator, TransactionScopeKind.None));
        }

        private IFinalProcess ProcessCreator()
        {
            File.Delete(OutputFileName);

            return new OperationHostProcess(Context, "OperationsHost")
            {
                InputProcess = new EpPlusExcelReaderProcess(Context, "Read:People")
                {
                    FileName = SourceFileName,
                    SheetName = "People",
                    ColumnConfiguration = new List<ReaderColumnConfiguration>()
                    {
                        new ReaderColumnConfiguration("Name", new StringConverter(formatProviderHint: CultureInfo.InvariantCulture)),
                        new ReaderColumnConfiguration("Age", new IntConverterAuto(formatProviderHint: CultureInfo.InvariantCulture)),
                    },
                },
                Operations = new List<IRowOperation>()
                {
                    new EpPlusSimpleRowWriterOperation()
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
            };
        }
    }
}