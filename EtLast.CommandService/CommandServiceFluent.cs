namespace FizzCode.EtLast;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class CommandServiceFluent
{
    public static IServiceCollection AddEtlCommandService(this IServiceCollection services, Func<CommandService> serviceCreator)
    {
        var service = serviceCreator.Invoke();
        services.AddHostedService(serviceProvider =>
        {
            var hostLifetime = serviceProvider.GetRequiredService<IHostApplicationLifetime>();
            service.HostLifetime = hostLifetime;
            return service;
        });

        return services;
    }

    public static T ClearReferenceAssemblyDirectories<T>(this T service)
        where T : CommandService
    {
        service.ReferenceAssemblyDirectories.Clear();
        return service;
    }

    public static T AddReferenceAssemblyDirectory<T>(this T service, string path)
        where T : CommandService
    {
        service.ReferenceAssemblyDirectories.Add(path);
        return service;
    }

    public static T UseServiceArgumentsDirectory<T>(this T service, string path)
        where T : CommandService
    {
        service.ServiceArgumentsDirectory = path;
        return service;
    }

    public static T UseModulesDirectory<T>(this T service, string path)
        where T : CommandService
    {
        service.ModulesDirectory = path;
        return service;
    }

    public static T SetModuleCompilationMode<T>(this T service, ModuleCompilationMode moduleCompilationMode)
        where T : CommandService
    {
        service.ModuleCompilationMode = moduleCompilationMode;
        return service;
    }

    public delegate void SessionBuilderAction(ISessionBuilder builder, IArgumentCollection sessionArguments);

    public static T ConfigureSession<T>(this T service, SessionBuilderAction builderAction)
        where T : CommandService
    {
        service.SessionConfigurators.Add(builderAction);
        return service;
    }
}