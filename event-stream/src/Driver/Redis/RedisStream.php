<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Driver\Redis;

use Generator;
use Hyperf\Redis\RedisFactory;
use Hyperf\Redis\RedisProxy;
use Menumbing\Contract\EventStream\IdProviderInterface;
use Menumbing\Contract\EventStream\StreamInterface;
use Menumbing\Contract\EventStream\StreamMessage;
use Menumbing\EventStream\Enum\ReadMessageFrom;
use Symfony\Component\Serializer\Serializer;

use function Hyperf\Support\now;

/**
 * @author  Iqbal Maulana <iq.bluejack@gmail.com>
 */
class RedisStream implements StreamInterface
{
    const NEW_ENTRY_KEY = '>';

    protected RedisProxy $redis;

    public function __construct(
        RedisFactory $redisFactory,
        protected Serializer $serializer,
        protected ?IdProviderInterface $id = null,
        protected array $options = []
    ) {
        $this->redis = $redisFactory->get($this->options['pool'] ?? 'default');
        $this->id = $this->id ?? new DefaultRedisId();
    }

    /**
     * @param  ReadMessageFrom|null  $readFrom  Overrides the `read_from` option for this group, e.g. for a group that
     *                                          must read entries written before it existed.
     */
    public function createGroup(string $name, string $stream, ?ReadMessageFrom $readFrom = null): bool
    {
        if ($this->hasGroup($name, $stream)) {
            return false;
        }

        $readFrom = match ($readFrom ?? $this->options['read_from'] ?? ReadMessageFrom::GROUP_CREATED) {
            ReadMessageFrom::GROUP_CREATED => '$',
            ReadMessageFrom::BEGINNING => '0',
        };

        return $this->redis->xgroup('CREATE', $stream, $name, $readFrom, true);
    }

    public function publish(StreamMessage $message): string
    {
        $stream = $message->stream;
        $messageId = $this->redis->xadd(
            $stream,
            $message->id ?? $this->id->newId(),
            ['message' => $this->serializer->serialize($message, $this->options['serialize_format'])]
        );

        if ($retention = $this->options['retention_period'] ?? null) {
            $maxId = now()->modify(sprintf('-%d days', $retention))->getTimestampMs() . '-0';

            $this->redis->xtrim($stream, $maxId, $this->options['approx'] ?? false, true);
        }

        return $messageId;
    }

    public function subscribe(string $consumer, string $group, array $streams): Generator
    {
        $waitTime = $this->options['wait_time'] ?? 100;

        $entries = $this->redis->xreadgroup($group, $consumer, array_fill_keys($streams, static::NEW_ENTRY_KEY), 1, $waitTime);

        if (false === $entries) {
            throw new \RuntimeException('Failed to read stream');
        }

        foreach ($entries as $stream => $messages) {
            foreach ($messages as $id => $message) {
                yield $stream => $this->deserialize($id, $message);
            }
        }
    }

    public function getIdleMessages(string $consumer, string $group, array $streams, int $retryAfter): Generator
    {
        foreach ($streams as $stream) {
            $pending = $this->redis->xpending($stream, $group, '-', '+', 1);

            foreach ($pending as $entry) {
                [$id, $owner, $idle, $retryCount] = array_values($entry);

                if ($idle >= $retryAfter) {
                    $claimed = $this->redis->xclaim($stream, $group, $consumer, $retryAfter, [$id], ['JUSTID' => false]);

                    foreach ($claimed as $id => $message) {
                        $streamMessage = $this->deserialize($id, $message, $retryCount);
                        $streamMessage = $streamMessage->withContext(['owner' => $owner]);

                        yield $stream => $streamMessage;
                    }
                }
            }
        }
    }

    public function ack(string $group, string $stream, array $ids): bool
    {
        return !!$this->redis->xack($stream, $group, $ids);
    }

    protected function hasGroup(string $name, string $stream): bool
    {
        if (false === $groups = $this->redis->xinfo('GROUPS', $stream)) {
            return false;
        }

        return in_array($name, array_column($groups, 'name'), true);
    }

    protected function deserialize(string $id, array $message, int $retryCount = 0): StreamMessage
    {
        $streamMessage = $this->serializer->deserialize($message['message'], StreamMessage::class, $this->options['deserialize_format'] ?? 'json');
        $streamMessage = $streamMessage->withContext(['retry_count' => $retryCount]);

        return $streamMessage->withId($id);
    }
}
