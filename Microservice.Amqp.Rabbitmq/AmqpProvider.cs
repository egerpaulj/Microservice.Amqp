//      Microservice AMQP Libraries for .Net C#                                                                                                                                       
//      Copyright (C) 2021  Paul Eger                                                                                                                                                                     
                                                                                                                                                                                                                   
//      This program is free software: you can redistribute it and/or modify                                                                                                                                          
//      it under the terms of the GNU General Public License as published by                                                                                                                                          
//      the Free Software Foundation, either version 3 of the License, or                                                                                                                                             
//      (at your option) any later version.                                                                                                                                                                           
                                                                                                                                                                                                                   
//      This program is distributed in the hope that it will be useful,                                                                                                                                               
//      but WITHOUT ANY WARRANTY; without even the implied warranty of                                                                                                                                                
//      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the                                                                                                                                                 
//      GNU General Public License for more details.                                                                                                                                                                  
                                                                                                                                                                                                                   
//      You should have received a copy of the GNU General Public License                                                                                                                                             
//      along with this program.  If not, see <https://www.gnu.org/licenses/>.

using System.Threading.Tasks;
using LanguageExt;
using Microservice.Amqp.Configuration;
using Microservice.Amqp.Rabbitmq.Configuration;
using System.Linq;
using System;
using Microsoft.Extensions.Configuration;
using Microservice.Serialization;

namespace Microservice.Amqp.Rabbitmq
{
    public class AmqpProvider : IAmqpProvider
    {
        private readonly RabbitmqConfig _configuration;
        private readonly AmqpConfiguration _amqpConfiguration;
        private readonly IJsonConverterProvider _converterProvider;
        private readonly IRabbitMqConnectionFactory _rabbitMqConnectionFactory;

        public AmqpProvider(IConfiguration configuration, IJsonConverterProvider converterProvider, IRabbitMqConnectionFactory rabbitMqConnectionFactory)
        {
            _rabbitMqConnectionFactory = rabbitMqConnectionFactory;
            _amqpConfiguration = new AmqpConfiguration(configuration);
            _configuration = LoadRabbitmqConfiguration(configuration);
            _converterProvider = converterProvider;
        }

        public TryOptionAsync<IMessagePublisher> GetPublisher(Option<string> contextName)
        {
            return
            contextName.ToTryOptionAsync()
            .Bind((Func<string, TryOptionAsync<IMessagePublisher>>)(context => async () =>
            {
                var amqpContext = GetContext(context);

                var publisherConfig = new RabbitMqPublisherConfig
                {
                    Host = _configuration.Host,
                    VirtHost = _configuration.VirtHost,
                    Username = _configuration.Username,
                    Password = _configuration.Password,
                    Exchange = amqpContext.Exchange,
                    RoutingKey = amqpContext.RoutingKey,
                    Context = context
                };

                return await Task.FromResult(new MessagePublisher(publisherConfig, _rabbitMqConnectionFactory, _converterProvider));
            }));
        }

        public TryOptionAsync<IMessageSubscriber<T, R>> GetSubsriber<T, R>(Option<string> contextName, IMessageHandler<T, R> messageHandler)
        {
            return
            contextName.ToTryOptionAsync()
            .Bind<string, IMessageSubscriber<T, R>>(context => async () =>
               {
                   var amqpContext = GetContext(context);
                   var subscriberConfig = new RabbitMqSubscriberConfig
                   {
                       Host = _configuration.Host,
                       VirtHost = _configuration.VirtHost,
                       Username = _configuration.Username,
                       Password = _configuration.Password,
                       QueueName = amqpContext.QueueName,
                   };

                   return await Task.FromResult(new MessageSubscriber<T, R>(
                                        subscriberConfig, 
                                        _converterProvider, 
                                        _rabbitMqConnectionFactory, 
                                        messageHandler));
               });
        }

        private AmqpContextConfiguration GetContext(string context)
        {
            var match = _amqpConfiguration.AmqpContexts.FirstOrDefault(c => c.Name == context);

            if (match == null)
            {
                throw new Exception($"Failed to find configuration for AMQP context: {context}");
            }

            return match;
        }

        internal static RabbitmqConfig LoadRabbitmqConfiguration(IConfiguration configuration)
        {
            var section = configuration
                                    .GetSection(AmqpConfiguration.AmqpConfigurationRoot)
                                    .GetSection("Provider")
                                    .GetChildren()
                                    .FirstOrDefault();

            if (section == null)
            {
                throw new Exception("Configuraiton missing for AMQP RabbitMq Provider");
            }

            return new RabbitmqConfig
            {
                Host = section.GetValue<string>("Host"),
                VirtHost = section.GetValue<string>("VirtHost"),
                Port = section.GetValue<int>("Port"),
                Username = section.GetValue<string>("Username"),
                Password = section.GetValue<string>("Password"),

            };
        }
    }
}