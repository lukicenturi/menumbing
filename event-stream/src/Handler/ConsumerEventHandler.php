<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Handler;

use Hyperf\Context\Context;
use Menumbing\Contract\EventStream\StreamMessage;
use Menumbing\EventStream\Enum\Result;
use Menumbing\EventStream\EventRegistry;
use Psr\EventDispatcher\EventDispatcherInterface;
use Symfony\Component\Serializer\Serializer;
use Throwable;

/**
 * @author  Iqbal Maulana <iq.bluejack@gmail.com>
 */
class ConsumerEventHandler
{
    public const NACK_REASON = self::class . '.nack_reason';

    public function __construct(
        protected EventDispatcherInterface $event,
        protected EventRegistry $eventRegistry,
        protected Serializer $serializer,
    ) {
    }

    public function __invoke(string $groupName, StreamMessage $message): Result
    {
        if (!$this->eventRegistry->has($message->type)) {
            return Result::ACK;
        }

        $metadata = $this->eventRegistry->getClassByName($message->type);

        try {
            return $this->handle($message, $metadata);
        } catch (\Throwable $e) {
            $retryCount = $message->context['retry_count'] ?? 0;

            if ($retryCount >= $metadata['retries']) {
                throw $e;
            }

            // Coroutine context, not a property: one handler instance serves every coroutine of a batch.
            Context::set(self::NACK_REASON, $e);

            return Result::NACK;
        }
    }

    public static function takeNackReason(): ?Throwable
    {
        $reason = Context::get(self::NACK_REASON);
        Context::destroy(self::NACK_REASON);

        return $reason instanceof Throwable ? $reason : null;
    }

    protected function handle(StreamMessage $message, array $metadata): Result
    {
        $this->event->dispatch(
            $this->serializer->denormalize($message->data, $metadata['class'])
        );

        return Result::ACK;
    }
}
