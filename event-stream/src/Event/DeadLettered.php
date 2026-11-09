<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Event;

use Menumbing\Contract\EventStream\StreamMessage;
use Throwable;

/**
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */
final class DeadLettered
{
    public function __construct(
        public readonly string $consumerName,
        public readonly string $groupName,
        public readonly string $streamName,
        public readonly string $deadLetterStream,
        public readonly StreamMessage $message,
        public readonly int $attempts,
        public readonly Throwable $throwable,
    ) {
    }
}
