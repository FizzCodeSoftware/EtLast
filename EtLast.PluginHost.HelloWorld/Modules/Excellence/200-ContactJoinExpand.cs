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
            Context.ExecuteOne(true, new BasicScope(Context, null, "People")
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

            yield return ProcessCreatorInternal(scope);
        }

        private IExecutable ProcessCreatorInternal(IExecutable scope)
        {
            return new OperationHostProcess(Context, "Process", scope.Topic)
            {
                InputProcess = new EpPlusExcelReaderProcess(Context, "Read", scope.Topic)
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
                        NoMatchAction = new NoMatchAction(MatchMode.Remove),
                        RightProcess = new OperationHostProcess(Context, "PrefilterInvalidContacts", scope.Topic)
                        {
                            InputProcess = new EpPlusExcelReaderProcess(Context, "ReadContacts", scope.Topic)
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
                        NoMatchAction = new NoMatchAction(MatchMode.Remove),
                        RightProcess = new EpPlusExcelReaderProcess(Context, "ReadContactMethod", scope.Topic)
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