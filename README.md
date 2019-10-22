# EtLast
###### ETL (Extract, Transform and Load) library for .NET Core 3

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

1) Create a new .NET Core Console Appplication

2) Add the following nuget packages:
- FizzCode.EtLast.PluginHost
- FizzCode.EtLast.PluginHost.HelloWorld

3) Replace your program.cs content with this code:

```cs
namespace HelloWorldApp
{
    internal static class Program
    {
        private static void Main(string[] args)
        {
            FizzCode.EtLast.PluginHost.CommandLineHandler.Run("HelloWorld", args);
        }
    }
}
```

4) start the application

5) type this and press Enter: run module HelloWorld