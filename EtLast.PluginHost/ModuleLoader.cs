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
        public static List<IEtlPlugin> LoadModule(ILogger logger, ILogger opsLogger, string moduleFolder, string sharedFolder, string nameSpaceEnding)
        {
            var startedOn = Stopwatch.StartNew();

            if (Debugger.IsAttached)
            {
                logger.Write(LogEventLevel.Information, "loading plugins directly from AppDomain where namespace ends with {NameSpaceEnding}", nameSpaceEnding);
                var appDomainPlugins = LoadPluginsFromAppDomain(nameSpaceEnding);
                logger.Write(LogEventLevel.Information, "finished in {Elapsed}", startedOn.Elapsed);
                return appDomainPlugins;
            }

            logger.Write(LogEventLevel.Information, "compiling plugins from {FolderName} using shared files in {SharedFolderName}", moduleFolder, sharedFolder);
            var selfFolder = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            var references = new List<MetadataReference>();

            var referenceDllFileNames = Directory.GetFiles(@"c:\Program Files\dotnet\packs\Microsoft.NETCore.App.Ref\3.0.0\ref\netcoreapp3.0", "*.dll", SearchOption.TopDirectoryOnly);
            foreach (var dllFileName in referenceDllFileNames)
            {
                references.Add(MetadataReference.CreateFromFile(dllFileName));
            }

            var localDllFileNames = Directory.GetFiles(selfFolder, "*.dll", SearchOption.TopDirectoryOnly);
            foreach (var dllFileName in localDllFileNames)
            {
                if (dllFileName.IndexOf("Microsoft.CodeDom.Providers.DotNetCompilerPlatform.dll", StringComparison.InvariantCultureIgnoreCase) > -1)
                    continue;
                if (dllFileName.IndexOf("Serilog", StringComparison.InvariantCultureIgnoreCase) > -1)
                    continue;

                references.Add(MetadataReference.CreateFromFile(dllFileName));
            }

            var fileNames = Directory.GetFiles(moduleFolder, "*.cs", SearchOption.AllDirectories);

            if (Directory.Exists(sharedFolder))
            {
                fileNames = fileNames
                    .Concat(Directory.GetFiles(sharedFolder, "*.cs", SearchOption.AllDirectories))
                    .ToArray();
            }

            var options = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.CSharp8);

            var trees = fileNames
                .Select(x => SyntaxFactory.ParseSyntaxTree(SourceText.From(File.ReadAllText(x)), options, x))
                .ToArray();

            using (var peStream = new MemoryStream())
            {
                var x = CSharpCompilation.Create("compiled.dll", trees, references, new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary,
                    optimizationLevel: OptimizationLevel.Release,
                    assemblyIdentityComparer: DesktopAssemblyIdentityComparer.Default));

                var result = x.Emit(peStream);
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

                peStream.Seek(0, SeekOrigin.Begin);

                var assemblyLoadContext = new SimpleUnloadableAssemblyLoadContext();
                var assembly = assemblyLoadContext.LoadFromStream(peStream);

                var compiledPlugins = LoadPluginsFromAssembly(assembly);
                logger.Write(LogEventLevel.Information, "finished in {Elapsed}", startedOn.Elapsed);
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

        private static List<IEtlPlugin> LoadPluginsFromAppDomain(string subFolder)
        {
            var result = new List<IEtlPlugin>();
            var pluginInterfaceType = typeof(IEtlPlugin);
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                foreach (var foundType in assembly.GetTypes().Where(x => pluginInterfaceType.IsAssignableFrom(x) && x.IsClass && !x.IsAbstract && x.Namespace.EndsWith(subFolder, StringComparison.OrdinalIgnoreCase)))
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

        internal class SimpleUnloadableAssemblyLoadContext : AssemblyLoadContext
        {
            public SimpleUnloadableAssemblyLoadContext()
                : base(true)
            {
            }

            protected override Assembly Load(AssemblyName assemblyName)
            {
                return null;
            }
        }
    }
}
