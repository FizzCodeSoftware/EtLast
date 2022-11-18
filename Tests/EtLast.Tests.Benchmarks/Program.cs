using FizzCode.EtLast.Benchmarks;

// note: Microsoft.CodeAnalysis.Analyzers must be added as nuget reference
// to prevent warnings caused by a transitive package reference to Microsoft.CodeAnalysis.Analyzers, version 2.6.2-beta2
// via the BechmarkDotNet -> Microsoft.CodeAnalysis.CSharp -> Microsoft.CodeAnalysis.Common -> Microsoft.CodeAnalysis.Analyzers path

BenchmarkRunner.Run<ReadFromDelimitedTests>();

Console.ReadLine();
