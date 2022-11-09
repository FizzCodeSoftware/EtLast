namespace FizzCode.EtLast.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, targetCount: 3)]
public class ReadFromDelimitedTestsInternal
{
    [Benchmark]
    public void ReadFromDelimitedFile()
    {
        var context = new EtlContext(null);
        var process = new DelimitedLineReader(context)
        {
            Delimiter = '\t',
            StreamProvider = new LocalFileStreamProvider()
            {
                FileName = @"h:\data\syngenta\PhenomenaShare\2019.csv",
            },
            //DefaultColumns = new TextReaderColumn(new DoubleConverter()).ValueWhenConversionFailed(null),
            Columns = new()
            {
                ["TrialId"] = new TextReaderColumn().FromSource("trial_id"),
                ["AbbreviatedCode"] = new TextReaderColumn().FromSource("abbreviated_code"),
                ["HighName"] = new TextReaderColumn().FromSource("highname"),
                ["MaterialId"] = new TextReaderColumn().FromSource("material_id"),
                ["Year"] = new TextReaderColumn(new IntConverter()).FromSource("year"),
                ["EntryNumber"] = new TextReaderColumn(new IntConverter()).FromSource("entry_no"),
                ["IsCheck"] = new TextReaderColumn(new BoolConverterAuto()).FromSource("is_check"),
                ["Notes"] = new TextReaderColumn().FromSource("NOTET"),
            },
            Header = DelimitedLineHeader.HasHeader,
        };

        //foreach (var row in process.TakeRowsAndReleaseOwnership(null))
        //Console.WriteLine(row.ToDebugString(true));

        process.Execute(null);
    }

    [Benchmark]
    public void ReadFromDelimitedFileOld()
    {
        var context = new EtlContext(null);
        var process = new DelimitedLineReaderOld(context)
        {
            Delimiter = '\t',
            StreamProvider = new LocalFileStreamProvider()
            {
                FileName = @"h:\data\syngenta\PhenomenaShare\2019.csv",
            },
            Columns = new()
            {
                ["TrialId"] = new ReaderColumn().FromSource("trial_id"),
                ["AbbreviatedCode"] = new ReaderColumn().FromSource("abbreviated_code"),
                ["HighName"] = new ReaderColumn().FromSource("highname"),
                ["MaterialId"] = new ReaderColumn().FromSource("material_id"),
                ["Year"] = new ReaderColumn(new IntConverter()).FromSource("year"),
                ["EntryNumber"] = new ReaderColumn(new IntConverter()).FromSource("entry_no"),
                ["IsCheck"] = new ReaderColumn(new BoolConverterAuto()).FromSource("is_check"),
                ["Notes"] = new ReaderColumn().FromSource("NOTET"),
            },
            Header = DelimitedLineHeader.HasHeader,
        };

        process.Execute(null);
    }
}