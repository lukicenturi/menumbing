<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Tests\Support;

use Closure;
use Menumbing\Contract\EventStream\StreamMessage;
use Menumbing\EventStream\Consumer\ConsumerProcess;
use Psr\Container\ContainerInterface;

/**
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */
final class TestableConsumerProcess extends ConsumerProcess
{
    /** @var list<int> */
    public array $pauses = [];

    /** @var list<StreamMessage> */
    public array $handled = [];

    public function __construct(
        ContainerInterface $container,
        private readonly Closure $script,
        string $streamName,
        string $groupName,
    ) {
        parent::__construct($container);

        $this->streamName = $streamName;
        $this->groupName = $groupName;
        $this->name = 'default:' . $streamName;
    }

    public function consume(StreamMessage $message): void
    {
        $this->processMessage($message);
    }

    protected function handler(): callable
    {
        return function (string $group, StreamMessage $message) {
            $this->handled[] = $message;

            return ($this->script)(count($this->handled), $message);
        };
    }

    protected function pause(int $milliseconds): void
    {
        $this->pauses[] = $milliseconds;
    }
}
