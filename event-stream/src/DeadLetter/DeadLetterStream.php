<?php

declare(strict_types=1);

namespace Menumbing\EventStream\DeadLetter;

/**
 * Builds the name of the dead-letter stream of one consumer group.
 *
 * The consumer (which writes the dead-letter stream) and the replay command
 * (which reads it) both get the name from here, so they always agree on it.
 *
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */
final class DeadLetterStream
{
    public const DEFAULT_SUFFIX = '.dlq';

    /**
     * `<stream>.<group><suffix>`, e.g. `user-events.billing-service.dlq`.
     *
     * Scoped to the consumer group because a source stream is often a fan-out
     * stream that several services read, each with its own group. A dead
     * letter belongs to the one group that failed it; a shared `<stream>.dlq`
     * would let the replay of one service pick up the failures of another.
     *
     * An empty suffix falls back to the default: without a suffix the name
     * would still differ from the source, but a suffix is what the replay
     * command uses to refuse a dead-letter name given as the source.
     */
    public static function nameFor(string $stream, string $group, string $suffix = self::DEFAULT_SUFFIX): string
    {
        return $stream . '.' . $group . ('' === $suffix ? self::DEFAULT_SUFFIX : $suffix);
    }
}
