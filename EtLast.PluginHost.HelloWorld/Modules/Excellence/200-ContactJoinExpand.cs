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
            Context.ExecuteOne(true, new BasicScope(PluginTopic)
            {
                ProcessCreator = ProcessCreator,
            });
        }

        private IEnumerable<IExecutable> ProcessCreator(BasicScope scope)
        {
            yield return new DeleteFile(scope.Topic, "DeleteFile")
            {
                FileName = OutputFileName,
            };

            yield return ProcessBuilder.Fluent
                .ReadFromExcel(new EpPlusExcelReader(scope.Topic, "PeopleReader")
                {
                    FileName = SourceFileName,
                    SheetName = "People",
                    ColumnConfiguration = new List<ReaderColumnConfiguration>()
                    {
                        new ReaderColumnConfiguration("ID", new IntConverterAuto(CultureInfo.InvariantCulture)),
                        new ReaderColumnConfiguration("Name", new StringConverter(CultureInfo.InvariantCulture)),
                    },
                })
                .Join(new JoinMutator(scope.Topic, "JoinContact")
                {
                    NoMatchAction = new NoMatchAction(MatchMode.Remove),
                    LookupBuilder = new RowLookupBuilder()
                    {
                        Process = ProcessBuilder.Fluent
                            .ReadFromExcel(new EpPlusExcelReader(scope.Topic, "ReadContacts")
                            {
                                FileName = SourceFileName,
                                SheetName = "Contact",
                                ColumnConfiguration = new List<ReaderColumnConfiguration>()
                                {
                                    new ReaderColumnConfiguration("PeopleID", new IntConverterAuto(CultureInfo.InvariantCulture)),
                                    new ReaderColumnConfiguration("MethodTypeID", new StringConverter(CultureInfo.InvariantCulture)),
                                    new ReaderColumnConfiguration("Value", new StringConverter(CultureInfo.InvariantCulture)),
                                },
                            })
                            .RemoveRow(new RemoveRowMutator(scope.Topic, "RemoveInvalidContacts")
                            {
                                If = row => row.IsNullOrEmpty("Value"),
                            })
                        .ProcessBuilder.Result,
                        KeyGenerator = row => row.GetAs<int>("PeopleID").ToString("D", CultureInfo.InvariantCulture),
                    },
                    RowKeyGenerator = row => row.GetAs<int>("ID").ToString("D", CultureInfo.InvariantCulture),
                    ColumnConfiguration = new List<ColumnCopyConfiguration>()
                    {
                        new ColumnCopyConfiguration("MethodTypeID"),
                        new ColumnCopyConfiguration("Value", "ContactValue"),
                    },
                })
                .Join(new JoinMutator(scope.Topic, "JoinContactMethod")
                {
                    NoMatchAction = new NoMatchAction(MatchMode.Remove),
                    LookupBuilder = new RowLookupBuilder()
                    {
                        Process = new EpPlusExcelReader(scope.Topic, "ReadContactMethod")
                        {
                            FileName = SourceFileName,
                            SheetName = "ContactMethod",
                            ColumnConfiguration = new List<ReaderColumnConfiguration>()
                            {
                                new ReaderColumnConfiguration("ID", new StringConverter(CultureInfo.InvariantCulture)), // used for "RightKey"
                                new ReaderColumnConfiguration("Name", new StringConverter(CultureInfo.InvariantCulture)), // will be renamed to "ContactMethod"
                            },
                        },
                        KeyGenerator = row => row.GetAs<string>("ID"),
                    },
                    RowKeyGenerator = row => row.GetAs<string>("MethodTypeID"),
                    ColumnConfiguration = new List<ColumnCopyConfiguration>()
                    {
                        new ColumnCopyConfiguration("Name", "ContactMethod"),
                    },
                })
                .WriteRowToExcelSimple(new EpPlusSimpleRowWriterMutator(scope.Topic, "Writer")
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
                })
                .ProcessBuilder.Result;
        }
    }
}