<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Tests\Support;

use Menumbing\EventStream\Annotation\ConsumedEvent;

/**
 * A consumed event class for the tests that run the real ConsumerEventHandler.
 *
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */
#[ConsumedEvent(stream: 'user-events', name: 'user.created')]
final class UserCreated
{
    public function __construct(
        public readonly string $userId,
        public readonly string $email,
    ) {
    }
}
