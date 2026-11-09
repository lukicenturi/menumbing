<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Tests\Support;

use Generator;
use Menumbing\Contract\EventStream\StreamInterface;
use Menumbing\Contract\EventStream\StreamMessage;
use RuntimeException;

/**
 * An in-memory stream with the one Kafka trait the consumer and the replay
 * command depend on: a message that is not acked comes back on the next
 * subscribe(). Publishing to a stream appends to it, so a message re-parked on
 * the dead-letter stream is read again too.
 *
 * `$pending` stands in for the Redis pending entries list: getIdleMessages()
 * hands those back until they are acked, with the `owner` context that
 * RedisStream adds.
 *
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */
final class FakeEventStream implements StreamInterface
{
    /** @var array<string, list<StreamMessage>> */
    public array $queues = [];

    /** @var array<string, list<StreamMessage>> */
    public array $pending = [];

    /** @var list<StreamMessage> */
    public array $published = [];

    /** @var list<array{group: string, stream: string, id: string}> */
    public array $acked = [];

    /** @var list<array{group: string, stream: string}> */
    public array $subscriptions = [];

    /** @var list<array{name: string, stream: string}> */
    public array $groups = [];

    /** @var list<string> publish() throws for messages bound to these streams. */
    public array $failPublishTo = [];

    /** ack() throws when set. */
    public bool $failAck = false;

    /** subscribe() throws on its first round when set, like a broker that is down. */
    public bool $failFirstSubscribe = false;

    private int $sequence = 0;

    public function __construct(private readonly int $batchSize = 100)
    {
    }

    public function push(StreamMessage $message): StreamMessage
    {
        $message = null === $message->id ? $message->withId(sprintf('generated-%04d', ++$this->sequence)) : $message;
        $this->queues[$message->stream][] = $message;

        return $message;
    }

    public function createGroup(string $name, string $stream): bool
    {
        $this->groups[] = ['name' => $name, 'stream' => $stream];

        return true;
    }

    public function publish(StreamMessage $message): string
    {
        if (in_array($message->stream, $this->failPublishTo, true)) {
            throw new RuntimeException(sprintf('Broker refused %s', $message->stream));
        }

        $message = $this->push($message);
        $this->published[] = $message;

        return (string) $message->id;
    }

    public function subscribe(string $consumer, string $group, array $streams): Generator
    {
        $this->subscriptions[] = ['group' => $group, 'stream' => implode(',', $streams)];

        if ($this->failFirstSubscribe && 1 === count($this->subscriptions)) {
            throw new RuntimeException('Connection refused');
        }

        $yielded = 0;

        foreach ($streams as $stream) {
            foreach ($this->queues[$stream] ?? [] as $message) {
                if ($yielded++ >= $this->batchSize) {
                    return;
                }

                yield $stream => $message->withContext(['retry_count' => 0]);
            }
        }
    }

    public function getIdleMessages(string $consumer, string $group, array $streams, int $retryAfter): Generator
    {
        foreach ($streams as $stream) {
            // One entry per call, like RedisStream (XPENDING with a count of 1).
            if (null !== $message = $this->pending[$stream][0] ?? null) {
                yield $stream => $message->withContext(['retry_count' => 1, 'owner' => 'consumer-that-crashed']);
            }
        }
    }

    public function ack(string $group, string $stream, array $ids): bool
    {
        if ($this->failAck) {
            throw new RuntimeException('Offset commit failed');
        }

        foreach ($ids as $id) {
            $this->acked[] = ['group' => $group, 'stream' => $stream, 'id' => $id];
            $this->remove($this->queues, $stream, $id);
            $this->remove($this->pending, $stream, $id);
        }

        return true;
    }

    /** @return list<string> */
    public function ackedIds(): array
    {
        return array_column($this->acked, 'id');
    }

    /** @return list<StreamMessage> */
    public function publishedTo(string $stream): array
    {
        return array_values(array_filter($this->published, static fn (StreamMessage $m): bool => $m->stream === $stream));
    }

    /**
     * @param  array<string, list<StreamMessage>>  $lists
     */
    private function remove(array &$lists, string $stream, string $id): void
    {
        foreach ($lists[$stream] ?? [] as $index => $message) {
            if ($message->id === $id) {
                unset($lists[$stream][$index]);
                $lists[$stream] = array_values($lists[$stream]);

                return;
            }
        }
    }
}
