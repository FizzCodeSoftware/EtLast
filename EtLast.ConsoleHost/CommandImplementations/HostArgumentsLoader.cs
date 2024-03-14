using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;

namespace FizzCode.EtLast;

internal static class HostArgumentsLoader
{
    public static IArgumentCollection LoadHostArguments(ConsoleHost host)
    {
        var argumentsDirectory = host.HostArgumentsDirectory;
        if (!Directory.Exists(argumentsDirectory))
            return new ArgumentCollection();

        var startedOn = Stopwatch.StartNew();

        var csFileNames = Directory.GetFiles(argumentsDirectory, "*.cs", SearchOption.AllDirectories).ToList();
        if (csFileNames.Count == 0)
            return new ArgumentCollection();

        host.Logger.Information("compiling host arguments from {Directory}", PathHelpers.GetFriendlyPathName(argumentsDirectory));

        var metadataReferences = host.GetReferenceAssemblyFilePaths()
            .Select(fn => MetadataReference.CreateFromFile(fn))
            .ToArray();

        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);
        var syntaxTrees = csFileNames
            .Select(fn => SyntaxFactory.ParseSyntaxTree(SourceText.From(File.ReadAllText(fn)), parseOptions, fn))
            .ToArray();

        using (var assemblyStream = new MemoryStream())
        {
            var compilationOptions = new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary,
                optimizationLevel: OptimizationLevel.Release,
                assemblyIdentityComparer: DesktopAssemblyIdentityComparer.Default);

            var compilation = CSharpCompilation.Create("compiled_host_arguments.dll", syntaxTrees, metadataReferences, compilationOptions);

            var result = compilation.Emit(assemblyStream);
            if (!result.Success)
            {
                var failures = result.Diagnostics.Where(diagnostic => diagnostic.IsWarningAsError || diagnostic.Severity == DiagnosticSeverity.Error);
                foreach (var error in failures)
                {
                    host.Logger.Write(LogEventLevel.Fatal, "syntax error in module: {ErrorMessage}", error.ToString());
                }

                return null;
            }

            assemblyStream.Seek(0, SeekOrigin.Begin);

            var assemblyLoadContext = new AssemblyLoadContext(null, isCollectible: true);
            var assembly = assemblyLoadContext.LoadFromStream(assemblyStream);

            var instanceConfigurationProviders = LoadInstancesFromAssembly<IInstanceArgumentProvider>(assembly);
            var defaultConfigurationProviders = LoadInstancesFromAssembly<IDefaultArgumentProvider>(assembly);
            host.Logger.Debug("compilation finished in {Elapsed}", startedOn.Elapsed);

            var instanceArgumentProviders = instanceConfigurationProviders;
            var defaultArgumentProviders = defaultConfigurationProviders;

            var instance = Environment.MachineName;
            var collection = new ArgumentCollection(defaultArgumentProviders, instanceArgumentProviders, instance, null);

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