<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Exception;

use RuntimeException;

/**
 * Stands in for the real error when the handler answered NACK instead of
 * throwing. ConsumerEventHandler does that while the event's
 * `#[ConsumedEvent(retries: n)]` budget is not used up, and it swallows the
 * exception when it does, so this is all that is left to report.
 *
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */
final class MessageNackedException extends RuntimeException
{
    public static function afterAttempt(int $attempt): self
    {
        return new self(sprintf('The consumer handler returned NACK on attempt %d.', $attempt));
    }
}
