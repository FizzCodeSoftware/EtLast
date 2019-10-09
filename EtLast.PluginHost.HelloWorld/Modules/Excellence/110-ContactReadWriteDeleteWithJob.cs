namespace FizzCode.EtLast.PluginHost.Excellence
{
    using System.Collections.Generic;
    using System.Globalization;
    using FizzCode.EtLast;
    using FizzCode.EtLast.EPPlus;

    public class ContactReadWriteDeleteWithJob : AbstractContactPlugin
    {
        public override void Execute()
        {
            Context.ExecuteOne(true, new OneProcessEtlStrategy(ProcessCreator, TransactionScopeKind.None));
        }

        private IFinalProcess ProcessCreator()
        {
            return new JobHostProcess(Context, "JobHost")
            {
                Jobs = new List<IJob>()
                {
                    new DeleteFileJob()
                    {
                         FileName = OutputFileName,
                    },
                    new EvaluateProcessWithoutResultJob()
                    {
                        Process = ProcessCreatorInternal(),
                    }
                },
            };
        }

        private OperationHostProcess ProcessCreatorInternal()
        {
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