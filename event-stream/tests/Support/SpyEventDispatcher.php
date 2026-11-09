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

use Psr\EventDispatcher\EventDispatcherInterface;

/**
 * Records what was dispatched and runs no listeners.
 */
final class SpyEventDispatcher implements EventDispatcherInterface
{
    /** @var list<object> */
    public array $events = [];

    public function dispatch(object $event): object
    {
        $this->events[] = $event;

        return $event;
    }

    /**
     * @template T of object
     * @param  class-string<T>  $class
     * @return list<T>
     */
    public function ofType(string $class): array
    {
        return array_values(array_filter($this->events, static fn (object $event): bool => $event instanceof $class));
    }
}
