<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://hyperf.wiki
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */
namespace Menumbing\EventStream\Tests\Support;

use Closure;
use Menumbing\Contract\EventStream\StreamMessage;
use Menumbing\EventStream\Consumer\ConsumerProcess;
use Psr\Container\ContainerInterface;

/**
 * The real retry and dead-letter logic with a scripted handler. Pauses are
 * recorded instead of slept.
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
