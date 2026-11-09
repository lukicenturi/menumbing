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

use Menumbing\EventStream\Annotation\ConsumedEvent;

/**
 * A consumed event class for the tests that run the real ConsumerEventHandler.
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
