namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Loader;
    using FizzCode.EtLast;
    using Microsoft.CodeAnalysis;
    using Microsoft.CodeAnalysis.CSharp;
    using Microsoft.CodeAnalysis.Text;
    using Serilog;
    using Serilog.Events;

    internal static class ModuleLoader
    {
        public static List<IEtlPlugin> LoadModule(ILogger logger, ILogger opsLogger, string moduleFolder, string sharedFolder, bool enableDynamicCompilation, string moduleName)
        {
            var startedOn = Stopwatch.StartNew();

            if (!enableDynamicCompilation || Debugger.IsAttached)
            {
                logger.Write(LogEventLevel.Information, "loading plugins directly from AppDomain if namespace ends with {ModuleName}", moduleName);
                var appDomainPlugins = LoadPluginsFromAppDomain(moduleName);
                logger.Write(LogEventLevel.Debug, "finished in {Elapsed}", startedOn.Elapsed);
                return appDomainPlugins;
            }

            logger.Write(LogEventLevel.Information, "compiling plugins from {ModuleFolder} using shared files from {SharedFolder}", PathHelpers.GetFriendlyPathName(moduleFolder), PathHelpers.GetFriendlyPathName(sharedFolder));
            var selfFolder = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            var referenceAssemblyFolder = @"c:\Program Files\dotnet\shared\Microsoft.NETCore.App\3.0.0";
            var referenceAssemblyPattern = "System*.dll";
            logger.Write(LogEventLevel.Information, "using reference assemblies from {ReferenceAssemblyFolder} using pattern: {ReferenceAssemblyPattern}", referenceAssemblyFolder, referenceAssemblyPattern);
            var referenceDllFileNames = Directory.GetFiles(referenceAssemblyFolder, referenceAssemblyPattern, SearchOption.TopDirectoryOnly);

            var referenceFileNames = new List<string>();
            referenceFileNames.AddRange(referenceDllFileNames);

            var localDllFileNames = Directory.GetFiles(selfFolder, "*.dll", SearchOption.TopDirectoryOnly);
            referenceFileNames.AddRange(localDllFileNames);

            var references = referenceFileNames.Distinct().Select(fn => MetadataReference.CreateFromFile(fn)).ToArray();

            var csFileNames = Directory.GetFiles(moduleFolder, "*.cs", SearchOption.AllDirectories);

            if (Directory.Exists(sharedFolder))
            {
                csFileNames = csFileNames
                    .Concat(Directory.GetFiles(sharedFolder, "*.cs", SearchOption.AllDirectories))
                    .ToArray();
            }

            var options = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.CSharp8);

            var syntaxTrees = csFileNames
                .Select(fn => SyntaxFactory.ParseSyntaxTree(SourceText.From(File.ReadAllText(fn)), options, fn))
                .ToArray();

            using (var assemblyStream = new MemoryStream())
            {
                var compilation = CSharpCompilation.Create("compiled.dll", syntaxTrees, references, new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary,
                    optimizationLevel: OptimizationLevel.Release,
                    assemblyIdentityComparer: DesktopAssemblyIdentityComparer.Default));

                var result = compilation.Emit(assemblyStream);
                if (!result.Success)
                {
                    var failures = result.Diagnostics.Where(diagnostic => diagnostic.IsWarningAsError || diagnostic.Severity == DiagnosticSeverity.Error);
                    foreach (var error in failures)
                    {
                        logger.Write(LogEventLevel.Error, "syntax error in plugin: {Message}", error.ToString());
                        opsLogger.Write(LogEventLevel.Error, "syntax error in plugin: {Message}", error.GetMessage());
                    }

                    return null;
                }

                assemblyStream.Seek(0, SeekOrigin.Begin);

                var assemblyLoadContext = new AssemblyLoadContext("PluginLoadContext", true);
                var assembly = assemblyLoadContext.LoadFromStream(assemblyStream);

                var compiledPlugins = LoadPluginsFromAssembly(assembly);
                logger.Write(LogEventLevel.Debug, "finished in {Elapsed}", startedOn.Elapsed);
                return compiledPlugins;
            }
        }

        private static List<IEtlPlugin> LoadPluginsFromAssembly(Assembly assembly)
        {
            var result = new List<IEtlPlugin>();
            var pluginInterfaceType = typeof(IEtlPlugin);
            foreach (var foundType in assembly.GetTypes().Where(x => pluginInterfaceType.IsAssignableFrom(x) && x.IsClass && !x.IsAbstract))
            {
                if (pluginInterfaceType.IsAssignableFrom(foundType) && foundType.IsClass && !foundType.IsAbstract)
                {
                    var plugin = (IEtlPlugin)Activator.CreateInstance(foundType, Array.Empty<object>());
                    if (plugin != null)
                    {
                        result.Add(plugin);
                    }
                }
            }

            return result;
        }

        private static List<IEtlPlugin> LoadPluginsFromAppDomain(string moduleName)
        {
            var plugins = new List<IEtlPlugin>();
            var pluginInterfaceType = typeof(IEtlPlugin);
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                var matchingTypes = assembly.GetTypes()
                    .Where(t => t.IsClass && !t.IsAbstract && pluginInterfaceType.IsAssignableFrom(t) && t.Namespace.EndsWith(moduleName, StringComparison.OrdinalIgnoreCase))
                    .ToList();

                foreach (var foundType in matchingTypes)
                {
                    var plugin = (IEtlPlugin)Activator.CreateInstance(foundType, Array.Empty<object>());
                    if (plugin != null)
                    {
                        plugins.Add(plugin);
                    }
                }
            }

            return plugins;
        }
    }
}