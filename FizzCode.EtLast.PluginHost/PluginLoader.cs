namespace FizzCode.EtLast.PluginHost
{
    using FizzCode.EtLast;
    using Serilog;
    using Serilog.Events;
    using System;
    using System.CodeDom.Compiler;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Reflection;

    internal class PluginLoader
    {
        public List<IEtlPlugin> LoadPlugins(ILogger logger, ILogger opsLogger, AppDomain domain, string folder, string subFolder)
        {
            var sw = Stopwatch.StartNew();

            var pluginInterfaceType = typeof(IEtlPlugin);
            var result = new List<IEtlPlugin>();

            if (Debugger.IsAttached)
            {
                logger.Write(LogEventLevel.Information, "loading plugins directly from AppDomain where namespace ends with {NameSpaceEnding}", subFolder);
                foreach (var assemby in domain.GetAssemblies())
                {
                    foreach (var foundType in assemby.GetTypes().Where(x => pluginInterfaceType.IsAssignableFrom(x) && x.IsClass && !x.IsAbstract && x.Namespace.EndsWith(subFolder, StringComparison.OrdinalIgnoreCase)))
                    {
                        var plugin = (IEtlPlugin)Activator.CreateInstance(foundType, new object[] { });
                        if (plugin != null)
                        {
                            result.Add(plugin);
                        }
                    }
                }

                logger.Write(LogEventLevel.Information, "finished in {Elapsed}", sw.Elapsed);
                return result;
            }

            logger.Write(LogEventLevel.Information, "compiling plugins from {FolderName}", folder);
            var selfFolder = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            // var provider = new CSharpCodeProvider(new Dictionary<string, string>() { { "CompilerVersion", "v4.0" } });
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
                if (dllFileName.IndexOf("Microsoft.CodeDom.Providers.DotNetCompilerPlatform.dll", StringComparison.InvariantCultureIgnoreCase) > -1) continue;
                if (dllFileName.IndexOf("Serilog", StringComparison.InvariantCultureIgnoreCase) > -1) continue;

                parameters.ReferencedAssemblies.Add(dllFileName);
            }

            /*parameters.ReferencedAssemblies.Add(Path.Combine(selfFolder, "EPPlus.dll"));
            parameters.ReferencedAssemblies.Add(Path.Combine(selfFolder, "FizzCode.EtLast.dll"));
            parameters.ReferencedAssemblies.Add(Path.Combine(selfFolder, "FizzCode.EtLast.Reference.dll"));
            parameters.ReferencedAssemblies.Add(Path.Combine(selfFolder, "FizzCode.EtLast.AdoNet.dll"));
            parameters.ReferencedAssemblies.Add(Path.Combine(selfFolder, "FizzCode.EtLast.EPPlus.dll"));
            parameters.ReferencedAssemblies.Add(Path.Combine(selfFolder, "FizzCode.EtLast.PluginHost.PluginInterface.dll"));*/
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

            foreach (var foundType in results.CompiledAssembly.GetTypes().Where(x => pluginInterfaceType.IsAssignableFrom(x) && x.IsClass && !x.IsAbstract))
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

            logger.Write(LogEventLevel.Information, "finished in {Elapsed}", sw.Elapsed);
            return result;
        }
    }
}
