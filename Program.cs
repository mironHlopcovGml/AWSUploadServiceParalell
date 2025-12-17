using AWSUploadService;
using Microsoft.Extensions.Logging.EventLog;
using WorkerUnTarService;

IHost host = Host.CreateDefaultBuilder(args)
     .ConfigureLogging(options =>
     {
         if (OperatingSystem.IsWindows())
         {
             options.AddFilter<EventLogLoggerProvider>(level => level >= LogLevel.Information);
         }
     })
    .ConfigureServices((hostContext, services) =>
    {
        services.AddHostedService<Worker>();
        if (OperatingSystem.IsWindows())
        {
            services.Configure<EventLogSettings>(config =>
            {
                if (OperatingSystem.IsWindows())
                {
                    config.LogName = "SRV.AWSUpload";
                    config.SourceName = "SRV.AWSUpload Source";
                }
            });
        }
        IConfiguration configuration = hostContext.Configuration;
        services.Configure<AWSUploadSettings>(configuration.GetSection(nameof(AWSUploadSettings)));
        services.Configure<AWS>(configuration.GetSection(nameof(AWS)));
    })
    .UseWindowsService()
    .Build();
host.Run();
