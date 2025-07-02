namespace KafkaConsumer;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

public class Program
{
    public static Task Main(string[] args)
    {
        return new HostBuilder()
           .ConfigureServices((hostContext, services) =>
           {
               services.AddSingleton<DbContextProvider>();
               services.AddHostedService<KafkaMessageConsumer>();
           }).Build().RunAsync();
    }
}
