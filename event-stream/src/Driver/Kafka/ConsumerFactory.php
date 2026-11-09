<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Driver\Kafka;

use Hyperf\Contract\ConfigInterface;
use longlang\phpkafka\Client\SwooleClient;
use longlang\phpkafka\Consumer\Consumer;
use longlang\phpkafka\Consumer\ConsumerConfig;
use longlang\phpkafka\Socket\SwooleSocket;
use longlang\phpkafka\Timer\SwooleTimer;
use RuntimeException;

/**
 * @author  Iqbal Maulana <iq.bluejack@gmail.com>
 */
class ConsumerFactory
{
    /**
     * @var Consumer[]
     */
    protected array $consumers = [];

    public function __construct(protected ConfigInterface $config)
    {
    }

    public function get(string $poolName, array $options): Consumer
    {
        $cacheKey = $this->getCacheKey($poolName, $options);

        if (null !== $consumer = $this->consumers[$cacheKey] ?? null) {
            return $consumer;
        }

        $consumerClass = $this->getConsumerClass($options);
        $consumer = new $consumerClass($this->getConfig($poolName, $options));

        $this->consumers[$cacheKey] = $consumer;

        return $consumer;
    }

    public function release(string $poolName, array $options): void
    {
        unset($this->consumers[$this->getCacheKey($poolName, $options)]);
    }

    public function close(string $poolName, array $options): void
    {
        $cacheKey = $this->getCacheKey($poolName, $options);

        if (null === $consumer = $this->consumers[$cacheKey] ?? null) {
            return;
        }

        unset($this->consumers[$cacheKey]);
        $consumer->close();
    }

    /**
     * @return class-string<Consumer>
     */
    protected function getConsumerClass(array $options): string
    {
        return 'earliest' === ($options['offset_reset'] ?? null) ? EarliestOffsetConsumer::class : Consumer::class;
    }

    protected function getConfig(string $poolName, array $options): ConsumerConfig
    {
        if (null === $config =  $this->config->get('kafka.' . $poolName)) {
            throw new RuntimeException(sprintf('Kafka pool "%s" is not defined.', $poolName));
        }

        $consumerConfig = new ConsumerConfig();
        $consumerConfig->setAutoCommit($options['auto_commit'] ?? true);
        $consumerConfig->setRackId($config['rack_id']);
        $consumerConfig->setReplicaId($config['replica_id']);
        $consumerConfig->setTopic($options['topic']);
        $consumerConfig->setRebalanceTimeout($config['rebalance_timeout']);
        $consumerConfig->setSendTimeout($config['send_timeout']);
        $groupId = $options['group_id'] ?? uniqid('hyperf-kafka-');

        $consumerConfig->setGroupId($groupId);
        $instanceId = $options['consumer_name'] ?? gethostname();
        $consumerConfig->setGroupInstanceId(
            sprintf('%s-%s-%s', $groupId, implode('_', (array) ($options['topic'] ?? [])), $instanceId)
        );
        $consumerConfig->setMemberId($options['member_id'] ?? '');
        $consumerConfig->setInterval($config['interval']);
        $consumerConfig->setBootstrapServers($config['bootstrap_servers']);
        $consumerConfig->setClient($config['client'] ?? SwooleClient::class);
        $consumerConfig->setSocket($config['socket'] ?? SwooleSocket::class);
        $consumerConfig->setTimer($config['timer'] ?? SwooleTimer::class);
        $consumerConfig->setMaxWriteAttempts($config['max_write_attempts']);
        $consumerConfig->setClientId(sprintf('%s-%s', $config['client_id'] ?: 'Hyperf', uniqid()));
        $consumerConfig->setRecvTimeout($config['recv_timeout']);
        $consumerConfig->setConnectTimeout($config['connect_timeout']);
        $consumerConfig->setSessionTimeout($config['session_timeout']);
        $consumerConfig->setGroupRetry($config['group_retry']);
        $consumerConfig->setGroupRetrySleep($config['group_retry_sleep']);
        $consumerConfig->setGroupHeartbeat($config['group_heartbeat']);
        $consumerConfig->setOffsetRetry($config['offset_retry']);
        $consumerConfig->setAutoCreateTopic($config['auto_create_topic']);
        $consumerConfig->setPartitionAssignmentStrategy($config['partition_assignment_strategy']);
        isset($config['min_bytes']) && $consumerConfig->setMinBytes($config['min_bytes']);
        isset($config['max_wait']) && $consumerConfig->setMaxWait($config['max_wait']);
        ! empty($config['sasl']) && $consumerConfig->setSasl($config['sasl']);
        ! empty($config['ssl']) && $consumerConfig->setSsl($config['ssl']);
        is_callable($config['exception_callback'] ?? null) && $consumerConfig->setExceptionCallback($config['exception_callback']);

        return $consumerConfig;
    }

    protected function getCacheKey(string $poolName, array $options): string
    {
        $topics = $options['topic'] ?? [];
        sort($topics);

        // Include consumer_name (instance ID) in cache key so that processes>1
        // on the same pod each get their own Consumer instance with unique group_instance_id.
        $instanceId = $options['consumer_name'] ?? '';

        return $poolName . ':' . implode(',', $topics) . ':' . $instanceId;
    }
}
