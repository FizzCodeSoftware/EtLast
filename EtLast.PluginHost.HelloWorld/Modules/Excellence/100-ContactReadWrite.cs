namespace FizzCode.EtLast.PluginHost.Excellence
{
    using System.Collections.Generic;
    using System.Globalization;
    using FizzCode.EtLast;

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
            yield return new DeleteFile(scope.Topic, "DeleteFile")
            {
                FileName = OutputFileName,
            };

            yield return ProcessBuilder.Fluent
                .ReadFromExcel(new EpPlusExcelReader(scope.Topic, "Reader")
                {
                    FileName = SourceFileName,
                    SheetName = "People",
                    ColumnConfiguration = new List<ReaderColumnConfiguration>()
                        {
                            new ReaderColumnConfiguration("Name", new StringConverter(CultureInfo.InvariantCulture)),
                            new ReaderColumnConfiguration("Age", new IntConverterAuto(CultureInfo.InvariantCulture)),
                        },
                })
                .WriteRowToExcelSimple(new EpPlusSimpleRowWriterMutator(scope.Topic, "Writer")
                {
                    FileName = OutputFileName,
                    SheetName = "output",
                    ColumnConfiguration = new List<ColumnCopyConfiguration>()
                        {
                            new ColumnCopyConfiguration("Name", "Contact name"),
                            new ColumnCopyConfiguration("Age", "Contact age"),
                        },
                    Finalize = (package, state) => state.LastWorksheet.Cells.AutoFitColumns(),
                })
                .ProcessBuilder.Result;
        }
    }
}