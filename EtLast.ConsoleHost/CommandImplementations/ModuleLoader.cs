namespace FizzCode.EtLast.ConsoleHost
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Runtime.Loader;
    using System.Threading;
    using FizzCode.EtLast;
    using Microsoft.CodeAnalysis;
    using Microsoft.CodeAnalysis.CSharp;
    using Microsoft.CodeAnalysis.Text;
    using Serilog.Events;

    internal static class ModuleLoader
    {
        private static long _moduleAutoincrementId;

        public static ExecutionResult LoadModule(CommandContext commandContext, string moduleName, bool forceCompilation, out CompiledModule module)
        {
            module = null;

            var moduleFolder = Path.Combine(commandContext.HostConfiguration.ModulesFolder, moduleName);
            if (!Directory.Exists(moduleFolder))
            {
                commandContext.Logger.Write(LogEventLevel.Fatal, "can't find the module folder: {Folder}", moduleFolder);
                return ExecutionResult.ModuleLoadError;
            }

            // read back actual folder name casing
            moduleFolder = Directory
                .GetDirectories(commandContext.HostConfiguration.ModulesFolder, moduleName, SearchOption.TopDirectoryOnly)
                .FirstOrDefault();

            moduleName = Path.GetFileName(moduleFolder);

            var startedOn = Stopwatch.StartNew();

            var useAppDomain = !forceCompilation && Debugger.IsAttached;
            if (useAppDomain)
            {
                commandContext.Logger.Information("loading module directly from AppDomain where namespace ends with '{Module}'", moduleName);
                var appDomainTasks = LoadInstancesFromAppDomain<IEtlTask>(moduleName);
                var startup = LoadInstancesFromAppDomain<IStartup>(moduleName).FirstOrDefault();
                var configurationProviders = LoadInstancesFromAppDomain<IConfigurationProvider>(moduleName);
                var defaultConfigurationProviders = LoadInstancesFromAppDomain<IDefaultConfigurationProvider>(moduleName);
                commandContext.Logger.Debug("finished in {Elapsed}", startedOn.Elapsed);

                module = new CompiledModule()
                {
                    Name = moduleName,
                    Folder = moduleFolder,
                    Startup = startup,
                    ConfigurationProviders = configurationProviders,
                    DefaultConfigurationProviders = defaultConfigurationProviders,
                    Tasks = appDomainTasks.Where(x => x.Name != null).ToList(),
                    LoadContext = null,
                };

                commandContext.Logger.Debug("{FlowCount} flows(s) found: {Task}",
                    module.Tasks.Count(x => x is AbstractEtlFlow), module.Tasks.Where(x => x is AbstractEtlFlow).Select(task => task.Name).ToArray());

                commandContext.Logger.Debug("{TaskCount} task(s) found: {Task}",
                    module.Tasks.Count(x => x is not AbstractEtlFlow), module.Tasks.Where(x => x is not AbstractEtlFlow).Select(task => task.Name).ToArray());

                return ExecutionResult.Success;
            }

            commandContext.Logger.Information("compiling module from {Folder}", PathHelpers.GetFriendlyPathName(moduleFolder));
            var selfFolder = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);

            var referenceAssemblyFolder = commandContext.HostConfiguration.CustomReferenceAssemblyFolder;
            if (string.IsNullOrEmpty(referenceAssemblyFolder))
            {
                referenceAssemblyFolder = Directory.GetDirectories(@"c:\Program Files\dotnet\shared\Microsoft.NETCore.App\", "6.*")
                    .OrderByDescending(x => new DirectoryInfo(x).CreationTime)
                    .FirstOrDefault();
            }

            commandContext.Logger.Information("using assemblies from {ReferenceAssemblyFolder}", referenceAssemblyFolder);

            var referenceDllFileNames = new List<string>();
            referenceDllFileNames.AddRange(Directory.GetFiles(referenceAssemblyFolder, "System*.dll", SearchOption.TopDirectoryOnly));
            referenceDllFileNames.AddRange(Directory.GetFiles(referenceAssemblyFolder, "netstandard.dll", SearchOption.TopDirectoryOnly));

            var referenceFileNames = new List<string>();
            referenceFileNames.AddRange(referenceDllFileNames.Where(x => !Path.GetFileNameWithoutExtension(x).EndsWith("Native", StringComparison.InvariantCultureIgnoreCase)));

            var localDllFileNames = Directory.GetFiles(selfFolder, "*.dll", SearchOption.TopDirectoryOnly)
                .Where(x => Path.GetFileName(x) != "FizzCode.EtLast.ConsoleHost.dll"
                    && !Path.GetFileName(x).Equals("testhost.dll", StringComparison.InvariantCultureIgnoreCase));
            referenceFileNames.AddRange(localDllFileNames);

            /*foreach (var fn in referenceFileNames)
                Console.WriteLine(fn);*/

            var metadataReferences = referenceFileNames
                .Distinct()
                .Select(fn => MetadataReference.CreateFromFile(fn))
                .ToArray();

            var csFileNames = Directory.GetFiles(moduleFolder, "*.cs", SearchOption.AllDirectories);

            var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.CSharp10);
            var syntaxTrees = csFileNames
                .Select(fn => SyntaxFactory.ParseSyntaxTree(SourceText.From(File.ReadAllText(fn)), parseOptions, fn))
                .ToArray();

            using (var assemblyStream = new MemoryStream())
            {
                var id = Interlocked.Increment(ref _moduleAutoincrementId);
                var compilationOptions = new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary,
                    optimizationLevel: OptimizationLevel.Release,
                    assemblyIdentityComparer: DesktopAssemblyIdentityComparer.Default);

                var compilation = CSharpCompilation.Create("compiled_" + id.ToString("D", CultureInfo.InvariantCulture) + ".dll", syntaxTrees, metadataReferences, compilationOptions);

                var result = compilation.Emit(assemblyStream);
                if (!result.Success)
                {
                    var failures = result.Diagnostics.Where(diagnostic => diagnostic.IsWarningAsError || diagnostic.Severity == DiagnosticSeverity.Error);
                    foreach (var error in failures)
                    {
                        commandContext.Logger.Write(LogEventLevel.Fatal, "syntax error in module: {ErrorMessage}", error.ToString());
                    }

                    return ExecutionResult.ModuleLoadError;
                }

                assemblyStream.Seek(0, SeekOrigin.Begin);

                var assemblyLoadContext = new AssemblyLoadContext(null, isCollectible: true);
                var assembly = assemblyLoadContext.LoadFromStream(assemblyStream);

                var compiledTasks = LoadInstancesFromAssembly<IEtlTask>(assembly);
                var compiledStartup = LoadInstancesFromAssembly<IStartup>(assembly).FirstOrDefault();
                var configurationProviders = LoadInstancesFromAppDomain<IConfigurationProvider>(moduleName);
                var defaultConfigurationProviders = LoadInstancesFromAppDomain<IDefaultConfigurationProvider>(moduleName);
                commandContext.Logger.Debug("compilation finished in {Elapsed}", startedOn.Elapsed);

                module = new CompiledModule()
                {
                    Name = moduleName,
                    Folder = moduleFolder,
                    Startup = compiledStartup,
                    ConfigurationProviders = configurationProviders,
                    DefaultConfigurationProviders = defaultConfigurationProviders,
                    Tasks = compiledTasks.Where(x => x.Name != null).ToList(),
                    LoadContext = assemblyLoadContext,
                };

                commandContext.Logger.Debug("{FlowCount} flows(s) found: {Task}",
                    module.Tasks.Count(x => x is AbstractEtlFlow), module.Tasks.Where(x => x is AbstractEtlFlow).Select(task => task.Name).ToArray());

                commandContext.Logger.Debug("{TaskCount} task(s) found: {Task}",
                    module.Tasks.Count(x => x is not AbstractEtlFlow), module.Tasks.Where(x => x is not AbstractEtlFlow).Select(task => task.Name).ToArray());

                return ExecutionResult.Success;
            }
        }

        public static void UnloadModule(CommandContext commandContext, CompiledModule module)
        {
            commandContext.Logger.Debug("unloading module {Module}", module.Name);

            module.Tasks.Clear();

            module.LoadContext?.Unload();
        }

        private static List<T> LoadInstancesFromAssembly<T>(System.Reflection.Assembly assembly)
        {
            var result = new List<T>();
            var interfaceType = typeof(T);
            foreach (var foundType in assembly.GetTypes().Where(x => interfaceType.IsAssignableFrom(x) && x.IsClass && !x.IsAbstract))
            {
                if (interfaceType.IsAssignableFrom(foundType) && foundType.IsClass && !foundType.IsAbstract)
                {
                    var instance = (T)Activator.CreateInstance(foundType, Array.Empty<object>());
                    if (instance != null)
                        result.Add(instance);
                }
            }

            return result;
        }

        private static List<T> LoadInstancesFromAppDomain<T>(string moduleName)
        {
            var result = new List<T>();
            var interfaceType = typeof(T);
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                var matchingTypes = assembly.GetTypes()
                    .Where(t => t.IsClass && !t.IsAbstract && interfaceType.IsAssignableFrom(t) && t.Namespace.EndsWith(moduleName, StringComparison.OrdinalIgnoreCase))
                    .ToList();

                foreach (var foundType in matchingTypes)
                {
                    var instance = (T)Activator.CreateInstance(foundType, Array.Empty<object>());
                    if (instance != null)
                        result.Add(instance);
                }
            }

            return result;
        }
    }
}