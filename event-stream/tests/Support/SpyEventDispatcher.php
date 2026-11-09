<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Tests\Support;

use Psr\EventDispatcher\EventDispatcherInterface;

/**
 * Records what was dispatched and runs no listeners.
 *
 * @author  Luki Centuri <lukicenturi@gmail.com>
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
