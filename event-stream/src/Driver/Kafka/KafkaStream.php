<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Driver\Kafka;

use Generator;
use Hyperf\Kafka\Producer;
use Hyperf\Kafka\ProducerManager;
use longlang\phpkafka\Consumer\ConsumeMessage;
use longlang\phpkafka\Consumer\Consumer;
use Menumbing\Contract\EventStream\IdProviderInterface;
use Menumbing\Contract\EventStream\StreamInterface;
use Menumbing\Contract\EventStream\StreamMessage;
use Swoole\Coroutine;
use Symfony\Component\Serializer\Serializer;
use Throwable;

/**
 * @author  Iqbal Maulana <iq.bluejack@gmail.com>
 */
class KafkaStream implements StreamInterface
{
    protected Producer $producer;

    /**
     * @var ConsumeMessage[]
     */
    protected array $pendingMessages = [];

    protected ?string $consumerName = null;

    /**
     * Set the consumer name used for Kafka static membership (group_instance_id).
     * Called by ConsumerProcess before subscribe() — the name is derived from
     * stream name + hostname + process index, making it deterministic and stable
     * across pod restarts (instant rejoin without rebalance).
     */
    public function setConsumerName(string $consumerName): void
    {
        $this->consumerName = $consumerName;
    }

    public function __construct(
        ProducerManager $producerManager,
        protected ConsumerFactory $consumerFactory,
        protected Serializer $serializer,
        protected ?IdProviderInterface $id = null,
        protected array $options = [],
    ) {
        $this->producer = $producerManager->getProducer($this->options['pool'] ?? 'default');
        $this->id = $this->id ?? new DefaultKafkaId();
    }

    public function createGroup(string $name, string $stream): bool
    {
        return true;
    }

    public function publish(StreamMessage $message): string
    {
        $stream = $message->stream;
        $messageId = $message->id ?? $this->id->newId();

        $this->producer->send(
            $stream,
            $this->serializer->serialize($message, $this->options['serialize_format']),
            $messageId
        );

        return $messageId;
    }

    public function subscribe(string $consumer, string $group, array $streams): Generator
    {
        $poolName = $this->options['pool'] ?? 'default';
        $consumerOptions = $this->getConsumerOptions($group, $streams);
        $kafkaConsumer = $this->consumerFactory->get($poolName, $consumerOptions);

        $waitTimeout = $this->options['wait_time'] ?? 100;
        $start = microtime(true);

        try {
            // Wait for at least 1 message to arrive
            while (true) {
                if (null !== $message = $kafkaConsumer->consume()) {
                    break;
                }

                $elapsed = (microtime(true) - $start) * 1000;

                if ($elapsed >= $waitTimeout) {
                    break;
                }

                Coroutine::sleep(0.005);
            }
        } catch (Throwable $e) {
            $this->consumerFactory->release($poolName, $consumerOptions);

            throw $e;
        }

        if (null === $message) {
            return;
        }

        // Yield the first message
        $key = $message->getTopic() . '.' . $message->getKey();
        $this->pendingMessages[$key] = $message;
        yield $message->getTopic() => $this->deserialize($message->getKey(), ['message' => $message->getValue()]);

        // Drain any remaining messages already buffered in-memory (no new fetchMessages() call).
        // We use Reflection to peek the private $messages buffer so we only drain what's already
        // fetched — avoiding a busy-loop from triggering another network round-trip.
        // With multiple partitions, a single fetchMessages() call returns records from all partitions
        // at once, allowing processBatch() to process them concurrently.
        $refProp = new \ReflectionProperty($kafkaConsumer, 'messages');
        $refProp->setAccessible(true);
        while ([] !== $refProp->getValue($kafkaConsumer)) {
            $message = $kafkaConsumer->consume();
            if (null === $message) {
                break;
            }
            $key = $message->getTopic() . '.' . $message->getKey();
            $this->pendingMessages[$key] = $message;
            yield $message->getTopic() => $this->deserialize($message->getKey(), ['message' => $message->getValue()]);
        }
    }

    public function getIdleMessages(string $consumer, string $group, array $streams, int $retryAfter): Generator
    {
        yield from [];
    }

    public function ack(string $group, string $stream, array $ids): bool
    {
        $consumer = $this->getConsumer($group, [$stream]);
        $count = 0;

        foreach ($ids as $id) {
            $key = $stream.'.'.$id;
            if (null !== $message = $this->pendingMessages[$key] ?? null) {
                $consumer->ack($message);
                unset($this->pendingMessages[$key]);
                ++$count;
            }
        }

        return count($ids) === $count;
    }

    /**
     * Leaves the consumer group of a subscription, so that a static member
     * does not hold its partitions until the session timeout. For short-lived
     * consumers such as a console command; a consumer process never leaves.
     */
    public function leaveGroup(string $group, array $streams): void
    {
        $this->consumerFactory->close($this->options['pool'] ?? 'default', $this->getConsumerOptions($group, $streams));
    }

    protected function deserialize(string $id, array $message, int $retryCount = 0): StreamMessage
    {
        $streamMessage = $this->serializer->deserialize($message['message'], StreamMessage::class, $this->options['deserialize_format'] ?? 'json');
        $streamMessage = $streamMessage->withContext(['retry_count' => $retryCount]);

        return $streamMessage->withId($id);
    }

    protected function getConsumer(string $group, array $streams): Consumer
    {
        return $this->consumerFactory->get($this->options['pool'] ?? 'default', $this->getConsumerOptions($group, $streams));
    }

    protected function getConsumerOptions(string $group, array $streams): array
    {
        // Kafka requires each consumer group to subscribe to the same set of topics.
        // Append stream name(s) to group_id so each stream gets its own isolated consumer group,
        // while still allowing multiple pods to share the same group per stream (scale-out).
        $groupId = $group . '-' . implode('_', $streams);

        $options = [
            ...$this->options,
            'group_id' => $groupId,
            'topic'    => $streams,
        ];

        // Forward consumer name for deterministic group_instance_id (static membership).
        // This enables instant rejoin on pod restart without waiting for session_timeout.
        if ($this->consumerName !== null) {
            $options['consumer_name'] = $this->consumerName;
        }

        return $options;
    }
}
