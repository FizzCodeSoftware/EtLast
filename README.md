# EtLast
###### ETL (Extract, Transform and Load) library for .Net Framework

For Examples, see the last chapter.

# License

See LICENSE.

# Contributing

Regarding pull requests or any contribution
- we need a signed CLA (Contributor License Agreement)
- only code which comply with .editorconfig is accepted

# NuGet packages
The master branch is automatically compiled and released to [nuget.org](https://www.nuget.org/packages?q=fizzcode.etlast)

# Examples

## Simple example

Sample to show how to set up and call a very simple Etl process.
This basic sample, the process makes no real sense in itself, since it simply repeats the created rows.

```cs
    // Create test sample row as object[][]
    var sampleRows = {
        new object[] { "1", "First" },
        new object[] { "2", "Second" },
        new object[] { "3", "Third" }
    };

    // Create Etl context
    var context = new EtlContext<DictionaryRow>();
    
    // Create producer process
    var producer = new CreateRowsProcess(context, "CreateRowsProcess")
    {
        Columns = new[] { "Id", "Name" },
        InputRows = sampleRows.ToList()
    };
    
    // Create operation process
    var process = new OperationHostProcess(context, "OperationProcess")
    {
        Configuration = new OperationHostProcessConfiguration()
        {
            WorkerCount = 2,
            MainLoopDelay = 10,
        },
        InputProcess = producer
    };
    
    // Get the result of the Etl
    var result = process.Evaluate();
```

## How to add two columns - introducing CustomOperation

```cs
    process.AddOperation(new CustomOperation()
    {
        Then = (customOpertation, row) => {
            row["ValueSum"] = row.GetAs<int>("ValueA") + row.GetAs<int>("ValueB");
        }
    });
```

## How to read from a csv file

```cs
    // DelimitedFileReaderProcess is a producer process. It will read from the provided csv file, with the configured columns and type converters.
    var delimitedFileReaderProcess = new DelimitedFileReaderProcess(context, "FromCsvToSqlProcess")
    {
        FileName = @"..\..\TestData\SampleHierarchy.csv",
        ColumnConfiguration = new List<ReaderColumnConfiguration>()
            {
                new ReaderColumnConfiguration("name","Name", new StringConverter(), string.Empty),
                new ReaderColumnConfiguration("level1","Level1", new StringConverter(), string.Empty),
                new ReaderColumnConfiguration("level2","Level2", new StringConverter(), string.Empty),
                new ReaderColumnConfiguration("level3","Level3", new StringConverter(), string.Empty),
            },
        HasHeaderRow = true
    };

    // Create operation process as before, only the InputProcess is the delimitedFileReaderProcess now.
    var process = new OperationHostProcess(context, "OperationProcess")
    {
        Configuration = new OperationHostProcessConfiguration()
        {
            WorkerCount = 2,
            MainLoopDelay = 10,
        },
        InputProcess = delimitedFileReaderProcess
    };
```

## How to write into a database

```cs
    process.AddOperation(new AdoNetWriteToTableOperation()
    {
        ConnectionStringKey = "MyConnection",
        SqlStatementCreator = new GenericInsertSqlStatementCreator
        {
            TableName = "TestTable",
            Columns = new[] { "Name", "Level1", "Level2", "Level3" }
        },
    });
```

You may find more examples in the unit tests.
Standalone, executable examples are coming soon.
