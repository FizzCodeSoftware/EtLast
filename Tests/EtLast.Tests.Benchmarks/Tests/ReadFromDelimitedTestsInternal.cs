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
            DefaultColumns = new TextReaderColumn(new DoubleConverter()).ValueWhenConversionFailed(null),
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
                ["year"] = new ReaderColumn(new IntConverter()),
                ["maturity_group"] = new ReaderColumn(new IntConverter()),
                ["planting_area_code"] = new ReaderColumn(new IntConverter()),
                ["planting_date_days"] = new ReaderColumn(new IntConverter()),
                ["location_code"] = new ReaderColumn(new IntConverter()),
                ["entry_no"] = new ReaderColumn(new IntConverter()),
            },
            DefaultColumns = new ReaderColumn(new DoubleConverter()).ValueWhenConversionFailed(null),
            Header = DelimitedLineHeader.HasHeader,
        };

        process.Execute(null);
    }
}