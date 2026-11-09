<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Tests\Support;

use Menumbing\EventStream\Driver\Kafka\EarliestOffsetConsumer;
use RuntimeException;

/**
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */
final class RecordingEarliestOffsetConsumer extends EarliestOffsetConsumer
{
    /** @var list<string> */
    public array $calls = [];

    public bool $failSeek = false;

    protected function joinGroup(): void
    {
        $this->calls[] = 'join';
    }

    protected function seekToEarliestWhereNotRetained(): void
    {
        $this->calls[] = 'seek';

        if ($this->failSeek) {
            throw new RuntimeException('ListOffsets timed out');
        }
    }

    protected function stopHeartbeat(): void
    {
        $this->calls[] = 'stop-heartbeat';
    }

    public function close(): void
    {
        $this->calls[] = 'close';
    }
}
