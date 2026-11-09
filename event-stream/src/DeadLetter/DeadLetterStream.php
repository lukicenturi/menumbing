<?php

declare(strict_types=1);

namespace Menumbing\EventStream\DeadLetter;

/**
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */
final class DeadLetterStream
{
    public const DEFAULT_SUFFIX = '.dlq';

    /**
     * Scoped to the group: a source stream is often read by several groups, and a dead letter belongs
     * to the one that failed it.
     */
    public static function nameFor(string $stream, string $group, string $suffix = self::DEFAULT_SUFFIX): string
    {
        return $stream . '.' . $group . ('' === $suffix ? self::DEFAULT_SUFFIX : $suffix);
    }
}
