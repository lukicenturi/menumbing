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
use Menumbing\EventStream\Command\DlqReplayCommand;

/**
 * The real command with its process-wide side effects replaced: the local
 * handler is scripted, and the runtime boot and timer clear are recorded
 * instead of run.
 */
final class TestableDlqReplayCommand extends DlqReplayCommand
{
    /** @var list<StreamMessage> */
    public array $dispatched = [];

    /** @var list<string> */
    public array $calls = [];

    /** @var list<string> */
    public array $unknownTypes = [];

    public ?Closure $script = null;

    protected function handler(): callable
    {
        return function (string $group, StreamMessage $message) {
            $this->dispatched[] = $message;

            return ($this->script)($message);
        };
    }

    protected function knowsType(string $type): bool
    {
        return !in_array($type, $this->unknownTypes, true);
    }

    protected function bootRuntime(): void
    {
        $this->calls[] = 'boot';
    }

    protected function leaveGroup(string $group, string $deadLetterStream): void
    {
        $this->calls[] = 'leave:' . $deadLetterStream;
        parent::leaveGroup($group, $deadLetterStream);
    }

    protected function clearTimers(): void
    {
        $this->calls[] = 'clear-timers';
    }
}
