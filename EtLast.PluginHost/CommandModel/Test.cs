﻿namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using CommandDotNet.Attributes;
    using FizzCode.DbTools.Configuration;
    using FizzCode.EtLast.AdoNet;

#pragma warning disable CA1812 // Avoid uninstantiated internal classes
#pragma warning disable CA1822 // Mark members as static
    [ApplicationMetadata(Name = "test", Description = "test various things, like connection strings, modules, etc")]
    [SubCommand]
    public class Test
    {
        [ApplicationMetadata(Name = "modules", Description = "Tests one or more modules.")]
        public void ValidateModule(
        [Argument(Name = "names", Description = "The space-separated list of module names.")]List<string> moduleNames,
        [Option(LongName = "all", ShortName = "a")]bool all)
        {
            if (moduleNames == null || moduleNames.Count == 0)
            {
                if (!all)
                {
                    CommandLineHandler.DisplayHelp("test modules");
                    return;
                }
            }
            else if (all)
            {
                CommandLineHandler.DisplayHelp("test modules");
                return;
            }

            var commandContext = CommandLineHandler.Context;

            if (all)
            {
                moduleNames = ModuleLister.GetAllModules(CommandLineHandler.Context);
            }

            foreach (var moduleName in moduleNames)
            {
                CommandLineHandler.Context.Logger.Information("loading module {Module}", moduleName);

                var module = ModuleLoader.LoadModule(commandContext, moduleName, null, null);
                if (module != null)
                {
                    ModuleLoader.UnloadModule(commandContext, module);
                    commandContext.Logger.Information("validation {ValidationResult} for {Module}", "PASSED", moduleName);
                }
                else
                {
                    commandContext.Logger.Information("validation {ValidationResult} for {Module}", "FAILED", moduleName);
                }

                Console.WriteLine();
            }
        }

        [ApplicationMetadata(Name = "connection-strings", Description = "Tests connection strings.")]
        public void ValidateConnectionStrings(
        [Argument(Name = "names", Description = "The space-separated list of module names.")]List<string> moduleNames,
        [Option(LongName = "all", ShortName = "a")]bool all)
        {
            if (moduleNames == null || moduleNames.Count == 0)
            {
                if (!all)
                {
                    CommandLineHandler.DisplayHelp("test connection-strings");
                    return;
                }
            }
            else if (all)
            {
                CommandLineHandler.DisplayHelp("test connection-strings");
                return;
            }

            var commandContext = CommandLineHandler.Context;

            if (all)
            {
                moduleNames = ModuleLister.GetAllModules(CommandLineHandler.Context);
            }

            var allConnectionStrings = new List<ConnectionStringWithProvider>();
            var index = 0;
            foreach (var moduleName in moduleNames)
            {
                var moduleConfiguration = ModuleConfigurationLoader.LoadModuleConfiguration(commandContext, moduleName, null, null);
                if (moduleConfiguration == null)
                    continue;

                if (index == 0)
                {
                    var sharedCs = new ConnectionStringCollection();
                    sharedCs.LoadFromConfiguration(moduleConfiguration.Configuration, "ConnectionStrings:Shared");
                    if (sharedCs.All.Any())
                    {
                        commandContext.Logger.Information("connection strings for: {Module}", "Shared");
                        foreach (var cs in sharedCs.All)
                        {
                            commandContext.Logger.Information("\t{ConnectionStringKey}, {ProviderName}", cs.Name, cs.ProviderName);
                            allConnectionStrings.Add(cs);
                        }
                    }
                }

                commandContext.Logger.Information("connection strings for: {Module}", moduleName);

                var connectionStrings = new ConnectionStringCollection();
                connectionStrings.LoadFromConfiguration(moduleConfiguration.Configuration, "ConnectionStrings:Module");
                foreach (var cs in connectionStrings.All)
                {
                    commandContext.Logger.Information("\t{ConnectionStringKey}, {ProviderName}", cs.Name, cs.ProviderName);
                    allConnectionStrings.RemoveAll(x => x.Name == cs.Name);
                    allConnectionStrings.Add(cs);
                }

                index++;
            }

            commandContext.Logger.Information("relevant connection strings");
            var originalNames = allConnectionStrings
                .Select(x => x.Name.Split('-')[0])
                .Distinct()
                .ToList();

            foreach (var originalName in originalNames)
            {
                var connectionString = allConnectionStrings.Find(x => string.Equals(x.Name, originalName + "-" + Environment.MachineName, StringComparison.InvariantCultureIgnoreCase))
                    ?? allConnectionStrings.Find(x => string.Equals(x.Name, originalName, StringComparison.InvariantCultureIgnoreCase));

                var knownFields = connectionString.GetKnownConnectionStringFields();
                if (knownFields == null)
                {
                    commandContext.Logger.Information("\ttesting: {ConnectionStringKey}, {ProviderName}",
                        connectionString.Name, connectionString.ProviderName);
                }
                else
                {
                    var message = "\ttesting: {ConnectionStringKey}, {ProviderName}";
                    var args = new List<object>()
                    {
                        connectionString.Name,
                        connectionString.ProviderName
                    };

                    if (knownFields.Server != null)
                    {
                        message += ", Server: {Server}";
                        args.Add(knownFields.Server);
                    }

                    if (knownFields.Port != null)
                    {
                        message += ", Port: {Port}";
                        args.Add(knownFields.Port);
                    }

                    if (knownFields.Database != null)
                    {
                        message += ", Database: {Database}";
                        args.Add(knownFields.Database);
                    }

                    if (knownFields.IntegratedSecurity != null)
                    {
                        message += ", IntegratedSecurity: {IntegratedSecurity}";
                        args.Add(knownFields.IntegratedSecurity);
                    }

                    if (knownFields.UserId != null)
                    {
                        message += ", UserId: {UserId}";
                        args.Add(knownFields.UserId);
                    }

                    commandContext.Logger.Information(message, args.ToArray());
                }

                try
                {
                    ConnectionManager.TestConnection(connectionString);
                    commandContext.Logger.Information("\t\tPASSED");
                }
                catch (Exception ex)
                {
                    commandContext.Logger.Error(ex, "\t\tFAILED");
                }
            }

            commandContext.Logger.Information("connection string test(s) finished");
        }
    }
#pragma warning restore CA1812 // Avoid uninstantiated internal classes
#pragma warning restore CA1822 // Mark members as static
}