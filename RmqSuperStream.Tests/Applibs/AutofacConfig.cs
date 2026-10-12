
namespace RmqSuperStream.Tests.Applibs
{
    using System.Linq;
    using System.Reflection;
    using Autofac;
    using RmqSuperStream.Applibs;
    using RmqSuperStream.Model;

    internal static class AutofacConfig
    {
        private static IContainer container;

        public static IContainer Container
        {
            get
            {
                if (container == null)
                {
                    Register();
                }

                return container;
            }
        }

        private static void Register()
        {
            var builder = new ContainerBuilder();
            var asm = Assembly.GetExecutingAssembly();

            builder.RegisterAssemblyTypes(asm)
                .Where(t => t.IsAssignableTo<IRabbitMqPubSubHandler>())
                .Named<IPubSubHandler<RabbitMqEventStream>>(t => t.Name.Replace("Handler", string.Empty))
                .PropertiesAutowired(PropertyWiringOptions.AllowCircularDependencies)
                .SingleInstance();

            builder.RegisterType<RabbitMqProducer>()
                .As<IRabbitMqProducer>()
                .SingleInstance();

            container = builder.Build();
        }
    }
}
