using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;

namespace FizzCode.EtLast;

internal static class ModuleLoader
{
    private static long _moduleAutoincrementId;

    public static ExecutionStatusCode LoadModule(CommandService host, string moduleName, bool useAppDomain, bool discoverTasks, out CompiledModule module)
    {
        module = null;

        var moduleDirectory = Path.Combine(host.ModulesDirectory, moduleName);
        if (!Directory.Exists(moduleDirectory))
        {
            host.Logger.Write(LogEventLevel.Fatal, "can't find the module directory: {Directory}", moduleDirectory);
            return ExecutionStatusCode.ModuleLoadError;
        }

        // read back actual directory name casing
        moduleDirectory = Directory
            .GetDirectories(host.ModulesDirectory, moduleName, SearchOption.TopDirectoryOnly)
            .FirstOrDefault();

        moduleName = Path.GetFileName(moduleDirectory);

        var startedOn = Stopwatch.StartNew();

        ForceLoadLocalDllsToAppDomain();

        if (useAppDomain)
        {
            host.Logger.Information("loading module directly from AppDomain where namespace ends with '{Module}'", moduleName);

            var appDomainTasks = FindTypesFromAppDomain<IEtlTask>(moduleName)
                .Where(x => x.Name != null).ToList();
            var startup = LoadInstancesFromAppDomain<IStartup>(moduleName).FirstOrDefault();
            var instanceConfigurationProviders = LoadInstancesFromAppDomain<InstanceArgumentProvider>(moduleName);
            var defaultConfigurationProviders = LoadInstancesFromAppDomain<ArgumentProvider>(moduleName);
            host.Logger.Debug("finished in {Elapsed}", startedOn.Elapsed);

            if (startup == null)
                host.Logger.Warning("Can't find a startup class implementing {StartupName}", nameof(IStartup));

            module = new CompiledModule()
            {
                Name = moduleName,
                Directory = moduleDirectory,
                Startup = startup != null ? startup.BuildSession : null,
                InstanceArgumentProviders = instanceConfigurationProviders,
                DefaultArgumentProviders = defaultConfigurationProviders,
                TaskTypes = appDomainTasks,
                PreCompiledTaskTypes = discoverTasks
                    ? FindTypesFromAppDomain<IEtlTask>()
                        .Where(x => x.Name != null)
                        .ToList()
                    : [],
                LoadContext = null,
            };

            if (discoverTasks)
            {
                host.Logger.Debug("{TaskCount} module tasks found: {Task}",
                    module.TaskTypes.Count, module.TaskTypes.Select(task => task.Name).ToArray());

                host.Logger.Debug("{TaskCount} indirect tasks found: {Task}",
                    module.PreCompiledTaskTypes.Count, module.PreCompiledTaskTypes.Select(task => task.FullName).ToArray());
            }

            return ExecutionStatusCode.Success;
        }

        host.Logger.Information("compiling module from {Directory}", PathHelpers.GetFriendlyPathName(moduleDirectory));

        var metadataReferences = host.GetReferenceAssemblyFilePaths()
            .Select(fn => MetadataReference.CreateFromFile(fn))
            .ToArray();

        var csFileNames = Directory.GetFiles(moduleDirectory, "*.cs", SearchOption.AllDirectories).ToList();
        var globalCsFileName = Path.Combine(host.ModulesDirectory, "Global.cs");
        if (File.Exists(globalCsFileName))
            csFileNames.Add(globalCsFileName);

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
            .AppendLine("global using global::FizzCode;")
            .AppendLine("global using global::FizzCode.EtLast;")
            ;

        syntaxTrees.Add(SyntaxFactory.ParseSyntaxTree(SourceText.From(globalUsing.ToString()), parseOptions));

        using (var assemblyStream = new MemoryStream())
        {
            var id = Interlocked.Increment(ref _moduleAutoincrementId);
            var compilationOptions = new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary,
                optimizationLevel: OptimizationLevel.Debug,
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

            var compiledStartup = LoadInstancesFromAssembly<IStartup>(assembly).FirstOrDefault();
            var instanceConfigurationProviders = LoadInstancesFromAssembly<InstanceArgumentProvider>(assembly);
            var defaultConfigurationProviders = LoadInstancesFromAssembly<ArgumentProvider>(assembly);
            host.Logger.Debug("compilation finished in {Elapsed}", startedOn.Elapsed);

            if (compiledStartup == null)
                host.Logger.Warning("Can't find a startup class implementing {StartupName}", nameof(IStartup));

            module = new CompiledModule()
            {
                Name = moduleName,
                Directory = moduleDirectory,
                Startup = compiledStartup != null ? compiledStartup.BuildSession : null,
                InstanceArgumentProviders = instanceConfigurationProviders,
                DefaultArgumentProviders = defaultConfigurationProviders,
                TaskTypes = discoverTasks
                    ? FindTypesFromAssembly<IEtlTask>(assembly)
                        .Where(x => x.Name != null).ToList()
                    : [],
                PreCompiledTaskTypes = discoverTasks
                    ? FindTypesFromAppDomain<IEtlTask>()
                        .Where(x => x.Name != null).ToList()
                    : null,
                LoadContext = assemblyLoadContext,
            };

            if (discoverTasks)
            {
                host.Logger.Debug("{TaskCount} module tasks found: {Task}",
                    module.TaskTypes.Count, module.TaskTypes.Select(task => task.Name).ToArray());

                host.Logger.Debug("{TaskCount} precompiled tasks found: {Task}",
                    module.PreCompiledTaskTypes.Count, module.PreCompiledTaskTypes.Select(task => task.FullName).ToArray());
            }

            return ExecutionStatusCode.Success;
        }
    }

    private static void ForceLoadLocalDllsToAppDomain()
    {
        var selfDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
        var localDllFileNames = Directory.GetFiles(selfDirectory, "*.dll", SearchOption.TopDirectoryOnly)
            .Where(x =>
            {
                var fn = Path.GetFileName(x);
                if (fn.Equals("testhost.dll", StringComparison.InvariantCultureIgnoreCase))
                    return false;

                if (fn.StartsWith("Microsoft", StringComparison.InvariantCultureIgnoreCase))
                    return false;

                if (fn.StartsWith("System", StringComparison.InvariantCultureIgnoreCase))
                    return false;

                return true;
            });

        foreach (var fn in localDllFileNames)
        {
            var loadedAssemblies = AppDomain.CurrentDomain.GetAssemblies().Where(x => !x.IsDynamic);
            var match = false;
            foreach (var loadedAssembly in loadedAssemblies)
            {
                try
                {
                    if (string.Equals(fn, loadedAssembly.Location, StringComparison.InvariantCultureIgnoreCase))
                    {
                        match = true;
                        break;
                    }
                }
                catch (Exception)
                {
                }
            }

            if (!match)
            {
                Debug.WriteLine("loading " + fn);
                try
                {
                    Assembly.LoadFile(fn);
                }
                catch (Exception)
                {
                }
            }
        }
    }

    public static void UnloadModule(CommandService host, CompiledModule module)
    {
        host.Logger.Debug("unloading module {Module}", module.Name);

        module.TaskTypes.Clear();
        module.PreCompiledTaskTypes?.Clear();

        module.LoadContext?.Unload();
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

    private static List<T> LoadInstancesFromAppDomain<T>(string moduleName)
    {
        var result = new List<T>();
        var interfaceType = typeof(T);
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            try
            {
                var matchingTypes = assembly.GetTypes()
                    .Where(t => t.IsClass && !t.IsAbstract && interfaceType.IsAssignableFrom(t) && t.Namespace.EndsWith(moduleName, StringComparison.OrdinalIgnoreCase));

                foreach (var foundType in matchingTypes)
                {
                    var instance = (T)Activator.CreateInstance(foundType, []);
                    if (instance != null)
                        result.Add(instance);
                }
            }
            catch (Exception) { }
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

    /*private static void FindIndirectTypesFromAssemblyRecursive(Type interfaceType, AssemblyLoadContext loadContext, AssemblyName assemblyName, HashSet<AssemblyName> alreadyLoaded, List<Type> resultList)
    {
        if (alreadyLoaded.Contains(assemblyName))
            return;

        try
        {
            var assembly = loadContext.LoadFromAssemblyName(assemblyName);
            alreadyLoaded.Add(assemblyName);

            resultList.AddRange(assembly.GetTypes()
                .Where(x => interfaceType.IsAssignableFrom(x) && x.IsClass && !x.IsAbstract));
        }
        catch (Exception)
        {
            // ignore
        }
    }*/

    private static List<Type> FindTypesFromAppDomain<T>()
    {
        var result = new List<Type>();
        var interfaceType = typeof(T);
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            try
            {
                var matchingTypes = assembly.GetTypes()
                    .Where(t => t.IsClass && !t.IsAbstract && interfaceType.IsAssignableFrom(t));

                result.AddRange(matchingTypes);
            }
            catch (Exception) { }
        }

        return result;
    }

    private static List<Type> FindTypesFromAppDomain<T>(string moduleName)
    {
        var result = new List<Type>();
        var interfaceType = typeof(T);
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            try
            {
                var matchingTypes = assembly.GetTypes()
                    .Where(t => t.IsClass && !t.IsAbstract && interfaceType.IsAssignableFrom(t) && t.Namespace.EndsWith(moduleName, StringComparison.OrdinalIgnoreCase));

                result.AddRange(matchingTypes);
            }
            catch (Exception) { }
        }

        return result;
    }
}