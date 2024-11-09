using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;

namespace FizzCode.EtLast;

internal static class ServiceArgumentsLoader
{
    public static ArgumentCollection LoadServiceArguments(CommandService commandService)
    {
        var argumentsDirectory = commandService.ServiceArgumentsDirectory;
        if (!Directory.Exists(argumentsDirectory))
            return new ArgumentCollection();

        var startedOn = Stopwatch.StartNew();

        var csFileNames = Directory.GetFiles(argumentsDirectory, "*.cs", SearchOption.AllDirectories).ToList();
        if (csFileNames.Count == 0)
            return new ArgumentCollection();

        commandService.Logger.Information("compiling service arguments from {Directory}", PathHelpers.GetFriendlyPathName(argumentsDirectory));

        var metadataReferences = commandService.GetReferenceAssemblyFilePaths()
            .Select(fn => MetadataReference.CreateFromFile(fn))
            .ToArray();

        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);
        var syntaxTrees = csFileNames
            .ConvertAll(fn => SyntaxFactory.ParseSyntaxTree(SourceText.From(File.ReadAllText(fn)), parseOptions, fn));

        var globalUsing = new StringBuilder()
            .AppendLine("global using global::System;")
            .AppendLine("global using global::System.Collections.Generic;")
            .AppendLine("global using global::System.IO;")
            .AppendLine("global using global::System.Linq;")
            .AppendLine("global using global::System.Net.Http;")
            .AppendLine("global using global::System.Threading;")
            .AppendLine("global using global::System.Threading.Tasks;")
            .AppendLine("global using global::FizzCode.EtLast;")
            .AppendLine("global using global::FizzCode.LightWeight;");

        syntaxTrees.Add(SyntaxFactory.ParseSyntaxTree(SourceText.From(globalUsing.ToString()), parseOptions));

        using (var assemblyStream = new MemoryStream())
        {
            var compilationOptions = new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary,
                optimizationLevel: OptimizationLevel.Release,
                allowUnsafe: true,
                assemblyIdentityComparer: DesktopAssemblyIdentityComparer.Default);

            var compilation = CSharpCompilation.Create("compiled_service_arguments.dll", syntaxTrees, metadataReferences, compilationOptions);

            var result = compilation.Emit(assemblyStream);
            if (!result.Success)
            {
                var failures = result.Diagnostics.Where(diagnostic => diagnostic.IsWarningAsError || diagnostic.Severity == DiagnosticSeverity.Error);
                foreach (var error in failures)
                {
                    commandService.Logger.Write(LogEventLevel.Fatal, "syntax error in module: {ErrorMessage}", error.ToString());
                }

                return null;
            }

            assemblyStream.Seek(0, SeekOrigin.Begin);

            var assemblyLoadContext = new AssemblyLoadContext(null, isCollectible: true);
            var assembly = assemblyLoadContext.LoadFromStream(assemblyStream);

            var instanceConfigurationProviders = LoadInstancesFromAssembly<InstanceArgumentProvider>(assembly);
            var defaultConfigurationProviders = LoadInstancesFromAssembly<ArgumentProvider>(assembly);
            commandService.Logger.Debug("compilation finished in {Elapsed}", startedOn.Elapsed);

            var instanceArgumentProviders = instanceConfigurationProviders;
            var defaultArgumentProviders = defaultConfigurationProviders;

            var collection = new ArgumentCollection(defaultArgumentProviders, instanceArgumentProviders, null, null);

            assemblyLoadContext.Unload();

            return collection;
        }
    }

    private static List<T> LoadInstancesFromAssembly<T>(Assembly assembly)
    {
        var result = new List<T>();
        var interfaceType = typeof(T);
        foreach (var foundType in assembly.GetTypes().Where(x => interfaceType.IsAssignableFrom(x) && x.IsClass && !x.IsAbstract))
        {
            var instance = (T)Activator.CreateInstance(foundType, []);
            if (instance != null)
                result.Add(instance);
        }

        return result;
    }
}