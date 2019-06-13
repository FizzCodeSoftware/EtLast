namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.CodeDom.Compiler;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using FizzCode.EtLast;
    using Serilog;
    using Serilog.Events;

    internal class PluginLoader
    {
        public List<IEtlPlugin> LoadPlugins(ILogger logger, ILogger opsLogger, string folder, string nameSpaceEnding)
        {
            var sw = Stopwatch.StartNew();

            if (Debugger.IsAttached)
            {
                logger.Write(LogEventLevel.Information, "loading plugins directly from AppDomain where namespace ends with {NameSpaceEnding}", nameSpaceEnding);
                var appDomainPlugins = LoadPluginsFromAppDomain(nameSpaceEnding);
                logger.Write(LogEventLevel.Information, "finished in {Elapsed}", sw.Elapsed);
                return appDomainPlugins;
            }

            logger.Write(LogEventLevel.Information, "compiling plugins from {FolderName}", folder);
            var selfFolder = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            var provider = new Microsoft.CodeDom.Providers.DotNetCompilerPlatform.CSharpCodeProvider();
            var parameters = new CompilerParameters();
            parameters.ReferencedAssemblies.Add("System.dll");
            parameters.ReferencedAssemblies.Add("System.Core.dll");
            parameters.ReferencedAssemblies.Add("System.Configuration.dll");
            parameters.ReferencedAssemblies.Add("System.Data.dll");
            parameters.ReferencedAssemblies.Add("System.Data.DataSetExtensions.dll");
            parameters.ReferencedAssemblies.Add("System.Net.Http.dll");
            parameters.ReferencedAssemblies.Add("System.Drawing.dll");
            parameters.ReferencedAssemblies.Add("System.Runtime.dll");
            parameters.ReferencedAssemblies.Add("System.Runtime.Serialization.dll");
            parameters.ReferencedAssemblies.Add("System.Net.Http.dll");
            parameters.ReferencedAssemblies.Add("System.Security.dll");
            parameters.ReferencedAssemblies.Add("System.ServiceModel.dll");
            parameters.ReferencedAssemblies.Add("System.ServiceProcess.dll");
            parameters.ReferencedAssemblies.Add("System.Transactions.dll");
            parameters.ReferencedAssemblies.Add("System.Windows.Forms.dll");
            parameters.ReferencedAssemblies.Add("System.Xml.dll");
            parameters.ReferencedAssemblies.Add("System.Xml.Linq.dll");

            var dllFileNames = Directory.GetFiles(selfFolder, "*.dll", SearchOption.TopDirectoryOnly);
            foreach (var dllFileName in dllFileNames)
            {
                if (dllFileName.IndexOf("Microsoft.CodeDom.Providers.DotNetCompilerPlatform.dll", StringComparison.InvariantCultureIgnoreCase) > -1)
                    continue;
                if (dllFileName.IndexOf("Serilog", StringComparison.InvariantCultureIgnoreCase) > -1)
                    continue;

                parameters.ReferencedAssemblies.Add(dllFileName);
            }

            parameters.GenerateExecutable = false;
            parameters.GenerateInMemory = true;

            var fileNames = Directory.GetFiles(folder, "*.cs", SearchOption.AllDirectories);

            var results = provider.CompileAssemblyFromFile(parameters, fileNames);
            if (results.Errors.Count > 0)
            {
                foreach (CompilerError error in results.Errors)
                {
                    logger.Write(LogEventLevel.Error, "syntax error in plugin: {Message}" + error.ToString());
                    opsLogger.Write(LogEventLevel.Error, "syntax error in plugin: {Message}" + error.ToString());
                }

                return null;
            }

            var compiledPlugins = LoadPluginsFromAssembly(results.CompiledAssembly);

            logger.Write(LogEventLevel.Information, "finished in {Elapsed}", sw.Elapsed);
            return compiledPlugins;
        }

        private static List<IEtlPlugin> LoadPluginsFromAssembly(Assembly assembly)
        {
            var result = new List<IEtlPlugin>();
            var pluginInterfaceType = typeof(IEtlPlugin);
            foreach (var foundType in assembly.GetTypes().Where(x => pluginInterfaceType.IsAssignableFrom(x) && x.IsClass && !x.IsAbstract))
            {
                if (pluginInterfaceType.IsAssignableFrom(foundType) && foundType.IsClass && !foundType.IsAbstract)
                {
                    var plugin = (IEtlPlugin)Activator.CreateInstance(foundType, new object[] { });
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
                    var plugin = (IEtlPlugin)Activator.CreateInstance(foundType, new object[] { });
                    if (plugin != null)
                    {
                        result.Add(plugin);
                    }
                }
            }

            return result;
        }
    }
}
