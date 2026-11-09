<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Exception;

use RuntimeException;

/**
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */
final class MessageNackedException extends RuntimeException
{
    public static function afterAttempt(int $attempt): self
    {
        return new self(sprintf('The consumer handler returned NACK on attempt %d.', $attempt));
    }
}
