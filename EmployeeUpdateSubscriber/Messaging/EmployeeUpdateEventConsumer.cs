using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;

namespace EmployeeUpdateSubscriber.Messaging
{
    public class EmployeeUpdateEventConsumer : BackgroundService
    {
        private readonly ILogger<EmployeeUpdateEventConsumer> _logger;
        private readonly IConnectionFactory _connectionFactory;
        private IConnection _connection;
        private const string EXCHANGE_NAME = "rabbitmq-pubsub";
        private const string TOPIC_NAME = "employee-updated-topic";

        public EmployeeUpdateEventConsumer(ILogger<EmployeeUpdateEventConsumer> logger, IConnectionFactory connectionFactory)
        {
            _logger = logger;
            _connectionFactory = connectionFactory;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                _connection = await _connectionFactory.CreateConnectionAsync();
                var channel = await _connection.CreateChannelAsync();

                await channel.ExchangeDeclareAsync(exchange: EXCHANGE_NAME, type: ExchangeType.Topic);
                var queueDeclareResult = await channel.QueueDeclareAsync().ConfigureAwait(false);
                string actualQueueName = queueDeclareResult.QueueName;
                await channel.QueueBindAsync(queue: actualQueueName, exchange: EXCHANGE_NAME, routingKey: TOPIC_NAME);
                _logger.LogInformation($" [=>] Bound queue '{actualQueueName}' to exchange '{EXCHANGE_NAME}' with routing key '{TOPIC_NAME}'.");
                _logger.LogInformation(" [***] Waiting for messages...");

                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += async (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    _logger.LogInformation($" [=>] Received updated employee: {message}");
                    var employeeData = JsonSerializer.Deserialize<object>(message);
                    _logger.LogInformation(" [X] Successfully Received and Processed Employee Update event!");
                };

                await channel.BasicConsumeAsync(queue: actualQueueName, autoAck: true, consumer: consumer);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to start RabbitMQ consumer.");
                return;
            }

            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
    }
}
