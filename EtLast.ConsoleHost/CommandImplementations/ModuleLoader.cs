using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;

namespace FizzCode.EtLast.ConsoleHost;
internal static class ModuleLoader
{
    private static long _moduleAutoincrementId;

    public static ExecutionStatusCode LoadModule(Host host, string moduleName, bool forceCompilation, out CompiledModule module)
    {
        module = null;

        var moduleFolder = Path.Combine(host.ModulesFolder, moduleName);
        if (!Directory.Exists(moduleFolder))
        {
            host.Logger.Write(LogEventLevel.Fatal, "can't find the module folder: {Folder}", moduleFolder);
            return ExecutionStatusCode.ModuleLoadError;
        }

        // read back actual folder name casing
        moduleFolder = Directory
            .GetDirectories(host.ModulesFolder, moduleName, SearchOption.TopDirectoryOnly)
            .FirstOrDefault();

        moduleName = Path.GetFileName(moduleFolder);

        var startedOn = Stopwatch.StartNew();

        var useAppDomain = !forceCompilation && Debugger.IsAttached;
        if (useAppDomain)
        {
            host.Logger.Information("loading module directly from AppDomain where namespace ends with '{Module}'", moduleName);
            var appDomainTasks = FindTypesFromAppDomain<IEtlTask>(moduleName);
            var startup = LoadInstancesFromAppDomain<IStartup>(moduleName).FirstOrDefault();
            var instanceConfigurationProviders = LoadInstancesFromAppDomain<IInstanceArgumentProvider>(moduleName);
            var defaultConfigurationProviders = LoadInstancesFromAppDomain<IDefaultArgumentProvider>(moduleName);
            host.Logger.Debug("finished in {Elapsed}", startedOn.Elapsed);

            module = new CompiledModule()
            {
                Name = moduleName,
                Folder = moduleFolder,
                Startup = startup,
                InstanceArgumentProviders = instanceConfigurationProviders,
                DefaultArgumentProviders = defaultConfigurationProviders,
                TaskTypes = appDomainTasks.Where(x => x.Name != null).ToList(),
                LoadContext = null,
            };

            host.Logger.Debug("{FlowCount} flows(s) found: {Task}",
                module.TaskTypes.Count(x => x.IsAssignableTo(typeof(AbstractEtlFlow))), module.TaskTypes.Where(x => x.IsAssignableTo(typeof(AbstractEtlFlow))).Select(task => task.Name).ToArray());

            host.Logger.Debug("{TaskCount} task(s) found: {Task}",
                module.TaskTypes.Count(x => !x.IsAssignableTo(typeof(AbstractEtlFlow))), module.TaskTypes.Where(x => !x.IsAssignableTo(typeof(AbstractEtlFlow))).Select(task => task.Name).ToArray());

            return ExecutionStatusCode.Success;
        }

        host.Logger.Information("compiling module from {Folder}", PathHelpers.GetFriendlyPathName(moduleFolder));
        var selfFolder = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

        var referenceDllFileNames = new List<string>();
        foreach (var referenceAssemblyFolder in host.ReferenceAssemblyFolders)
        {
            var folder = Directory.GetDirectories(referenceAssemblyFolder, "6.*")
                .OrderByDescending(x => new DirectoryInfo(x).CreationTime)
                .FirstOrDefault();

            host.Logger.Information("using assemblies from {ReferenceAssemblyFolder}", folder);

            referenceDllFileNames.AddRange(Directory.GetFiles(folder, "System*.dll", SearchOption.TopDirectoryOnly));
            referenceDllFileNames.AddRange(Directory.GetFiles(folder, "Microsoft.AspNetCore*.dll", SearchOption.TopDirectoryOnly));
            referenceDllFileNames.AddRange(Directory.GetFiles(folder, "Microsoft.Extensions*.dll", SearchOption.TopDirectoryOnly));
            referenceDllFileNames.AddRange(Directory.GetFiles(folder, "Microsoft.Net*.dll", SearchOption.TopDirectoryOnly));
            referenceDllFileNames.AddRange(Directory.GetFiles(folder, "netstandard.dll", SearchOption.TopDirectoryOnly));
        }

        var referenceFileNames = new List<string>();
        referenceFileNames.AddRange(referenceDllFileNames.Where(x => !Path.GetFileNameWithoutExtension(x).EndsWith("Native", StringComparison.InvariantCultureIgnoreCase)));

        var localDllFileNames = Directory.GetFiles(selfFolder, "*.dll", SearchOption.TopDirectoryOnly)
            .Where(x => Path.GetFileName(x) != "FizzCode.EtLast.ConsoleHost.dll"
                && !Path.GetFileName(x).Equals("testhost.dll", StringComparison.InvariantCultureIgnoreCase));
        referenceFileNames.AddRange(localDllFileNames);

        var metadataReferences = referenceFileNames
            .Distinct()
            .Select(fn => MetadataReference.CreateFromFile(fn))
            .ToArray();

        var csFileNames = Directory.GetFiles(moduleFolder, "*.cs", SearchOption.AllDirectories).ToList();
        var globalCsFileName = Path.Combine(host.ModulesFolder, "Global.cs");
        if (File.Exists(globalCsFileName))
            csFileNames.Add(globalCsFileName);

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
                    host.Logger.Write(LogEventLevel.Fatal, "syntax error in module: {ErrorMessage}", error.ToString());
                }

                return ExecutionStatusCode.ModuleLoadError;
            }

            assemblyStream.Seek(0, SeekOrigin.Begin);

            var assemblyLoadContext = new AssemblyLoadContext(null, isCollectible: true);
            var assembly = assemblyLoadContext.LoadFromStream(assemblyStream);

            var compiledTasks = FindTypesFromAssembly<IEtlTask>(assembly);
            var compiledStartup = LoadInstancesFromAssembly<IStartup>(assembly).FirstOrDefault();
            var instanceConfigurationProviders = LoadInstancesFromAppDomain<IInstanceArgumentProvider>(moduleName);
            var defaultConfigurationProviders = LoadInstancesFromAppDomain<IDefaultArgumentProvider>(moduleName);
            host.Logger.Debug("compilation finished in {Elapsed}", startedOn.Elapsed);

            module = new CompiledModule()
            {
                Name = moduleName,
                Folder = moduleFolder,
                Startup = compiledStartup,
                InstanceArgumentProviders = instanceConfigurationProviders,
                DefaultArgumentProviders = defaultConfigurationProviders,
                TaskTypes = compiledTasks.Where(x => x.Name != null).ToList(),
                LoadContext = assemblyLoadContext,
            };

            host.Logger.Debug("{FlowCount} flows(s) found: {Task}",
                module.TaskTypes.Count(x => x.IsAssignableTo(typeof(AbstractEtlFlow))), module.TaskTypes.Where(x => x.IsAssignableTo(typeof(AbstractEtlFlow))).Select(task => task.Name).ToArray());

            host.Logger.Debug("{TaskCount} task(s) found: {Task}",
                module.TaskTypes.Count(x => !x.IsAssignableTo(typeof(AbstractEtlFlow))), module.TaskTypes.Where(x => !x.IsAssignableTo(typeof(AbstractEtlFlow))).Select(task => task.Name).ToArray());

            return ExecutionStatusCode.Success;
        }
    }

    public static void UnloadModule(Host host, CompiledModule module)
    {
        host.Logger.Debug("unloading module {Module}", module.Name);

        module.TaskTypes.Clear();

        module.LoadContext?.Unload();
    }

    private static List<T> LoadInstancesFromAssembly<T>(Assembly assembly)
    {
        var result = new List<T>();
        var interfaceType = typeof(T);
        foreach (var foundType in assembly.GetTypes().Where(x => interfaceType.IsAssignableFrom(x) && x.IsClass && !x.IsAbstract))
        {
            var instance = (T)Activator.CreateInstance(foundType, Array.Empty<object>());
            if (instance != null)
                result.Add(instance);
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
                .Where(t => t.IsClass && !t.IsAbstract && interfaceType.IsAssignableFrom(t) && t.Namespace.EndsWith(moduleName, StringComparison.OrdinalIgnoreCase));

            foreach (var foundType in matchingTypes)
            {
                var instance = (T)Activator.CreateInstance(foundType, Array.Empty<object>());
                if (instance != null)
                    result.Add(instance);
            }
        }

        return result;
    }

    private static List<Type> FindTypesFromAssembly<T>(Assembly assembly)
    {
        var interfaceType = typeof(T);
        return assembly.GetTypes()
            .Where(x => interfaceType.IsAssignableFrom(x) && x.IsClass && !x.IsAbstract)
            .ToList();
    }

    private static List<Type> FindTypesFromAppDomain<T>(string moduleName)
    {
        var result = new List<Type>();
        var interfaceType = typeof(T);
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            var matchingTypes = assembly.GetTypes()
                .Where(t => t.IsClass && !t.IsAbstract && interfaceType.IsAssignableFrom(t) && t.Namespace.EndsWith(moduleName, StringComparison.OrdinalIgnoreCase));

            result.AddRange(matchingTypes);
        }

        return result;
    }
}