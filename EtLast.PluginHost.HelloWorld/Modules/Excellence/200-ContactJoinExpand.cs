namespace FizzCode.EtLast.PluginHost.Excellence
{
    using System.Collections.Generic;
    using System.Globalization;
    using FizzCode.EtLast;
    using FizzCode.EtLast.EPPlus;

    public class ContactJoinExpand : AbstractContactPlugin
    {
        public override void Execute()
        {
            Context.ExecuteOne(true, new DefaultEtlStrategy(ProcessCreator, TransactionScopeKind.None));
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

        public IFinalProcess ProcessCreatorInternal()
        {
            return new OperationHostProcess(Context, "OperationsHost")
            {
                InputProcess = new EpPlusExcelReaderProcess(Context, "Read:People")
                {
                    FileName = SourceFileName,
                    SheetName = "People",
                    ColumnConfiguration = new List<ReaderColumnConfiguration>()
                    {
                        new ReaderColumnConfiguration("ID", new IntConverterAuto(formatProviderHint: CultureInfo.InvariantCulture)), // used for "LeftKey"
                        new ReaderColumnConfiguration("Name", new StringConverter(formatProviderHint: CultureInfo.InvariantCulture)),
                    },
                },
                Operations = new List<IRowOperation>()
                {
                    new JoinOperation()
                    {
                        NoMatchAction = new MatchAction(MatchMode.Remove),
                        RightProcess = new OperationHostProcess(Context, "PrefilterInvalidContacts")
                        {
                            InputProcess = new EpPlusExcelReaderProcess(Context, "Read:Contact")
                            {
                                FileName = SourceFileName,
                                SheetName = "Contact",
                                ColumnConfiguration = new List<ReaderColumnConfiguration>()
                                {
                                    new ReaderColumnConfiguration("PeopleID", new IntConverterAuto(formatProviderHint: CultureInfo.InvariantCulture)), // used for "RightKey"
                                    new ReaderColumnConfiguration("MethodTypeID", new StringConverter(formatProviderHint: CultureInfo.InvariantCulture)),
                                    new ReaderColumnConfiguration("Value", new StringConverter(formatProviderHint: CultureInfo.InvariantCulture)), // will be renamed to ContactValue
                                },
                            },
                            Operations = new List<IRowOperation>()
                            {
                                new RemoveRowOperation()
                                {
                                    If = row => row.IsNullOrEmpty("Value"),
                                },
                            }
                        },
                        LeftKeySelector = row => row.GetAs<int>("ID").ToString("D", CultureInfo.InvariantCulture),
                        RightKeySelector = row => row.GetAs<int>("PeopleID").ToString("D", CultureInfo.InvariantCulture),
                        ColumnConfiguration = new List<ColumnCopyConfiguration>()
                        {
                            new ColumnCopyConfiguration("MethodTypeID"),
                            new ColumnCopyConfiguration("Value", "ContactValue"),
                        },
                    },
                    new ExpandOperation()
                    {
                        NoMatchAction = new MatchAction(MatchMode.Remove),
                        RightProcess = new EpPlusExcelReaderProcess(Context, "Read:ContactMethod")
                        {
                            FileName = SourceFileName,
                            SheetName = "ContactMethod",
                            ColumnConfiguration = new List<ReaderColumnConfiguration>()
                            {
                                new ReaderColumnConfiguration("ID", new StringConverter(formatProviderHint: CultureInfo.InvariantCulture)), // used for "RightKey"
                                new ReaderColumnConfiguration("Name", new StringConverter(formatProviderHint: CultureInfo.InvariantCulture)), // will be renamed to "ContactMethod"
                            },
                        },
                        LeftKeySelector = row => row.GetAs<string>("MethodTypeID"),
                        RightKeySelector = row => row.GetAs<string>("ID"),
                        ColumnConfiguration = new List<ColumnCopyConfiguration>()
                        {
                            new ColumnCopyConfiguration("Name", "ContactMethod"),
                        },
                    },
                    new EpPlusSimpleRowWriterOperation()
                    {
                        FileName = OutputFileName,
                        SheetName = "output",
                        ColumnConfiguration = new List<ColumnCopyConfiguration>()
                        {
                            new ColumnCopyConfiguration("Name", "Contact name"),
                            new ColumnCopyConfiguration("ContactMethod", "Contact method"),
                            new ColumnCopyConfiguration("ContactValue", "Value"),
                        },
                        Finalize = (package, state) => state.LastWorksheet.Cells.AutoFitColumns(),
                    }
                },
            };
        }
    }
}