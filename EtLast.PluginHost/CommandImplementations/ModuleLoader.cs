namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Loader;
    using System.Threading;
    using FizzCode.EtLast;
    using Microsoft.CodeAnalysis;
    using Microsoft.CodeAnalysis.CSharp;
    using Microsoft.CodeAnalysis.Text;

    internal static class ModuleLoader
    {
        private static long _moduleAutoincrementId = 0;

        public static Module LoadModule(CommandContext commandContext, string moduleName, string[] moduleSettingOverrides, string[] pluginListOverride)
        {
            var moduleConfiguration = ModuleConfigurationLoader.LoadModuleConfiguration(commandContext, moduleName, moduleSettingOverrides, pluginListOverride);
            if (moduleConfiguration == null)
                return null;

            if (moduleConfiguration.ConnectionStrings.All.Any())
            {
                commandContext.Logger.Information("relevant connection strings for: {Module}", moduleName);

                foreach (var connectionString in moduleConfiguration.ConnectionStrings.All)
                {
                    var knownFields = connectionString.GetKnownConnectionStringFields();
                    if (knownFields == null)
                    {
                        commandContext.Logger.Information("\t{ConnectionStringKey} ({Provider})",
                            connectionString.Name, connectionString.GetFriendlyProviderName());
                    }
                    else
                    {
                        var message = "\t{ConnectionStringKey} ({Provider})";
                        var args = new List<object>()
                            {
                                connectionString.Name,
                                connectionString.GetFriendlyProviderName(),
                            };

                        if (knownFields.Server != null)
                        {
                            message += ", server: {Server}";
                            args.Add(knownFields.Server);
                        }

                        if (knownFields.Port != null)
                        {
                            message += ", port: {Port}";
                            args.Add(knownFields.Port);
                        }

                        if (knownFields.Database != null)
                        {
                            message += ", database: {Database}";
                            args.Add(knownFields.Database);
                        }

                        if (knownFields.IntegratedSecurity != null)
                        {
                            message += ", integrated security: {IntegratedSecurity}";
                            args.Add(knownFields.IntegratedSecurity);
                        }

                        if (knownFields.UserId != null)
                        {
                            message += ", user: {UserId}";
                            args.Add(knownFields.UserId);
                        }

                        commandContext.Logger.Information(message, args.ToArray());
                    }
                }
            }

            var sharedFolder = Path.Combine(commandContext.HostConfiguration.ModulesFolder, "Shared");
            var sharedConfigFileName = Path.Combine(sharedFolder, "shared-configuration.json");

            var startedOn = Stopwatch.StartNew();

            if (commandContext.HostConfiguration.DynamicCompilationMode == DynamicCompilationMode.Never
                || (commandContext.HostConfiguration.DynamicCompilationMode == DynamicCompilationMode.Default && Debugger.IsAttached))
            {
                commandContext.Logger.Information("loading plugins directly from AppDomain if namespace ends with '{Module}'", moduleName);
                var appDomainPlugins = LoadPluginsFromAppDomain(moduleName);
                commandContext.Logger.Debug("finished in {Elapsed}", startedOn.Elapsed);
                var module = new Module()
                {
                    ModuleConfiguration = moduleConfiguration,
                    Plugins = appDomainPlugins,
                    EnabledPlugins = FilterExecutablePlugins(moduleConfiguration, appDomainPlugins),
                };

                commandContext.Logger.Debug("{PluginCount} plugin(s) found: {PluginNames}",
                    module.EnabledPlugins.Count, module.EnabledPlugins.Select(plugin => TypeHelpers.GetFriendlyTypeName(plugin.GetType())).ToArray());

                return module;
            }

            commandContext.Logger.Information("compiling plugins from {Folder} using shared files from {SharedFolder}", PathHelpers.GetFriendlyPathName(moduleConfiguration.ModuleFolder), PathHelpers.GetFriendlyPathName(sharedFolder));
            var selfFolder = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            var referenceAssemblyFolder = @"c:\Program Files\dotnet\shared\Microsoft.NETCore.App\3.1.0";
            var referenceAssemblyPattern = "System*.dll";
            commandContext.Logger.Information("using reference assemblies from {ReferenceAssemblyFolder} using pattern: {ReferenceAssemblyPattern}", referenceAssemblyFolder, referenceAssemblyPattern);
            var referenceDllFileNames = Directory.GetFiles(referenceAssemblyFolder, referenceAssemblyPattern, SearchOption.TopDirectoryOnly);

            var referenceFileNames = new List<string>();
            referenceFileNames.AddRange(referenceDllFileNames);

            var localDllFileNames = Directory.GetFiles(selfFolder, "*.dll", SearchOption.TopDirectoryOnly);
            referenceFileNames.AddRange(localDllFileNames);

            var metadataReferences = referenceFileNames
                .Distinct()
                .Select(fn => MetadataReference.CreateFromFile(fn))
                .ToArray();

            var csFileNames = Directory.GetFiles(moduleConfiguration.ModuleFolder, "*.cs", SearchOption.AllDirectories);

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
                var id = Interlocked.Increment(ref _moduleAutoincrementId);
                var compilation = CSharpCompilation.Create("compiled_" + id.ToString("D", CultureInfo.InvariantCulture) + ".dll", syntaxTrees, metadataReferences, new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary,
                    optimizationLevel: OptimizationLevel.Release,
                    assemblyIdentityComparer: DesktopAssemblyIdentityComparer.Default));

                var result = compilation.Emit(assemblyStream);
                if (!result.Success)
                {
                    var failures = result.Diagnostics.Where(diagnostic => diagnostic.IsWarningAsError || diagnostic.Severity == DiagnosticSeverity.Error);
                    foreach (var error in failures)
                    {
                        // DiagnosticFormatter can be used for custom formatting
                        commandContext.Logger.Error("syntax error in plugin: {Message}", error.ToString());
                        commandContext.OpsLogger.Error("syntax error in plugin: {Message}", error.GetMessage());
                    }

                    return null;
                }

                assemblyStream.Seek(0, SeekOrigin.Begin);

                var assemblyLoadContext = new AssemblyLoadContext("loader", false);
                var assembly = assemblyLoadContext.LoadFromStream(assemblyStream);

                var compiledPlugins = LoadPluginsFromAssembly(assembly);
                commandContext.Logger.Debug("finished in {Elapsed}", startedOn.Elapsed);
                var module = new Module()
                {
                    ModuleConfiguration = moduleConfiguration,
                    Plugins = compiledPlugins,
                    EnabledPlugins = FilterExecutablePlugins(moduleConfiguration, compiledPlugins),
                };

                commandContext.Logger.Debug("{PluginCount} plugin(s) found: {PluginNames}",
                    module.EnabledPlugins.Count, module.EnabledPlugins.Select(plugin => TypeHelpers.GetFriendlyTypeName(plugin.GetType())).ToArray());

                return module;
            }
        }

        public static void UnloadModule(CommandContext commandContext, Module module)
        {
            commandContext.Logger.Debug("unloading module {Module}", module.ModuleConfiguration.ModuleName);

            module.ModuleConfiguration = null;
            module.Plugins = null;
            module.EnabledPlugins = null;
        }

        private static List<IEtlPlugin> FilterExecutablePlugins(ModuleConfiguration moduleConfiguration, List<IEtlPlugin> plugins)
        {
            if (plugins == null || plugins.Count == 0)
                return new List<IEtlPlugin>();

            return plugins
                .Where(plugin =>
                {
                    var pluginName = plugin.GetType().Name;
                    return moduleConfiguration.EnabledPluginList.Any(enabledName => string.Equals(enabledName, pluginName, StringComparison.InvariantCultureIgnoreCase));
                })
                .ToList();
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