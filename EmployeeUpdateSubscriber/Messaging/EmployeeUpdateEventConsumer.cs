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
        private const string QUEUE_NAME = "employee-read-dto-queue";

        public EmployeeUpdateEventConsumer(ILogger<EmployeeUpdateEventConsumer> logger, IConnectionFactory connectionFactory)
        {
            _logger = logger;
            _connectionFactory = connectionFactory;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _connection = await _connectionFactory.CreateConnectionAsync();
            var channel = await _connection.CreateChannelAsync();
            try
            {
                _logger.LogInformation(" [*] Attempting to connect to RabbitMQ...");

                await channel.ExchangeDeclareAsync(
                    exchange: EXCHANGE_NAME,
                    type: ExchangeType.Topic,
                    durable: true
                );

                await channel.QueueDeclareAsync(
                    queue: QUEUE_NAME,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null
                );

                await channel.QueueBindAsync(
                    queue: QUEUE_NAME,
                    exchange: EXCHANGE_NAME,
                    routingKey: TOPIC_NAME
                );

                _logger.LogInformation($" [✓] Bound queue '{QUEUE_NAME}' to exchange '{EXCHANGE_NAME}' with routing key '{TOPIC_NAME}'.");
                _logger.LogInformation(" [*] Waiting for messages...");

                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var jsonMessage = Encoding.UTF8.GetString(body);
                    _logger.LogError($" [<<] Received raw message payload: {jsonMessage}");

                    try
                    {
                        var employeeData = JsonSerializer.Deserialize<EmployeeReadDTO>(
                            jsonMessage,
                            new JsonSerializerOptions { PropertyNameCaseInsensitive = true }
                        );

                        if (employeeData != null)
                        {
                            _logger.LogInformation($" [✓] Successfully Received and Processed Employee Update event!");
                            _logger.LogInformation($"  -> ID: {employeeData.Id}");
                            _logger.LogInformation($"  -> Name: {employeeData.Name}");
                            _logger.LogInformation($"  -> Email: {employeeData.Email}");
                            _logger.LogInformation($"  -> Address: {employeeData.Address}");
                        }
                        else
                        {
                            _logger.LogWarning($" [!] Received message was null after deserialization. Raw: {jsonMessage}");
                        }
                        channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $" [!] Error processing message. Raw: {jsonMessage}");
                    }
                    return Task.CompletedTask;
                };

                await channel.BasicConsumeAsync(queue: QUEUE_NAME, autoAck: false, consumer: consumer);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Fatal error in RabbitMQ setup. Consumer stopped.");
            }

            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
    }
}
