# Microservice.Amqp

Generic library to setup AMQP and utilize EIP patterns using Queues.

Provides the following:
- Generic message publishers
- Generic message consumers
- IObservable<Either<R, Exception>> messages
- Bootstrap to setup Exchanges/Queues/Deadletter
- Integration Tests
- RabbitMq Implementation

## Using the library

### Configuration/Library
Several contexts can be defined for the library/application/service (see Presentation case study).

Each context name (e.g. CrawlResponse/CrawlRequest below), can be used to obtain:
- IAmqpProvider 
- IPublisher
- ISubscriber<T, R> Where T is the published message and R is the expected response 


'''
Amqp": {
    "Contexts": {
      "CrawlResponse": {
        "Exchange": "crawl",
        "Queue": "response_queue",
        "RoutingKey": "response*",
        "RetryCount": "0"
      },
      "CrawlRequest": {
        "Exchange": "crawl",
        "Queue": "request_queue",
        "RoutingKey": "requests*",
        "RetryCount": "0"
      }
    },
    "Provider": {
      "Rabbitmq" : {
        "Host": "localhost",
        "VirtHost": "/",
        "Port": "5671",
        "Username": "guest",
        "Password": "guest"
      }
    }
}
'''
### Usage
The following are the key interfaces:
- IPublisher: publishes message to the AMQP system
- ISubscriber<T, R>: handles messages from the AMQP system (input T, output R - using the IHandler<T, R>)
- IHandler<T, R>: handles business logic (input T, output R)
- IObservable<Either<R, Exception>>: can be used to connect observables
- IAmqpProvider: gets the Subcrivers/Publishers based on the context name

Note: MessageHandlerFactory can be used to create simple IHandler<T, R>. Or inject to your factory.
Note: See integration tests

E.g.
'''
var amqpProvider = new AmqpProvider(configuration, _jsonConverterProviderMock.Object, new RabbitMqConnectionFactory());
var amqpBootstrapper = new AmqpBootstrapper(configuration);

// The message handler is registered here converting/business-logic: T => R
var subscriberTryOption = await _amqpProvider.GetSubsriber<TestRequestMessage, string>
                                (
                                    "CrawlRequest",
                                    MessageHandlerFactory.Create<TestRequestMessage, string>(t => t.TestId));

var subcriber = subscriberTryOption.Match(p => p, () => throw new System.Exception("Subscriber missing"), ex => throw ex);

var publisher = _amqpProvider.GetPublisher("CrawlRequest").Match(p => p, () => throw new System.Exception("Publisher missing"));

await publisher.Publish<TestRequestMessage>(new TestRequestMessage{ TestId = $"TestCase: {i}  -  {Guid.NewGuid().ToString()}"})
                .Match(p => p, () => throw new Exception("publish failed"));

// Additionally Get Observables of Either<R, Exception> for event processing
subscriber.GetObservable().Subscribe(m => Console.WriteLine(m));
subscriber.Start();
'''

### Integration Tests
Start RabbitMq (see docker example file: startRabbitMqDocker.sh) and update the configuration file (if not localhost).

## License
Copyright (C) 2021  Paul Eger

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.