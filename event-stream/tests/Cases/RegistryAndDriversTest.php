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
namespace Menumbing\EventStream\Tests\Cases;

use Hyperf\Di\Annotation\AnnotationCollector;
use Hyperf\Redis\RedisFactory;
use Hyperf\Redis\RedisProxy;
use Menumbing\EventStream\Annotation\ConsumedEvent;
use Menumbing\EventStream\Driver\Kafka\ConsumerFactory;
use Menumbing\EventStream\Driver\Redis\RedisStream;
use Menumbing\EventStream\Enum\ReadMessageFrom;
use Menumbing\EventStream\EventRegistry;
use Menumbing\EventStream\Exception\EventNameConflictedException;
use Menumbing\EventStream\Tests\Support\UserCreated;
use Mockery;
use Symfony\Component\Serializer\Serializer;

/**
 * The small additions the replay command relies on outside the command.
 *
 * @internal
 * @coversNothing
 */
class RegistryAndDriversTest extends AbstractTestCase
{
    protected function tearDown(): void
    {
        Mockery::close();

        parent::tearDown();
    }

    public function testEnsureInitializedFillsTheRegistryOnce(): void
    {
        AnnotationCollector::collectClass(UserCreated::class, ConsumedEvent::class, new ConsumedEvent(stream: 'user-events', name: 'user.created', retries: 2));
        $registry = new EventRegistry();

        $registry->ensureInitialized();
        $registry->ensureInitialized();

        $this->assertSame(['class' => UserCreated::class, 'retries' => 2], $registry->getClassByName('user.created'));
    }

    public function testEnsureInitializedAfterInitDoesNotFillItAgain(): void
    {
        AnnotationCollector::collectClass(UserCreated::class, ConsumedEvent::class, new ConsumedEvent(stream: 'user-events', name: 'user.created'));
        $registry = new EventRegistry();
        $registry->init();

        $registry->ensureInitialized();

        $this->assertTrue($registry->has('user.created'));
    }

    public function testASecondInitStillThrows(): void
    {
        AnnotationCollector::collectClass(UserCreated::class, ConsumedEvent::class, new ConsumedEvent(stream: 'user-events', name: 'user.created'));
        $registry = new EventRegistry();
        $registry->init();

        $this->expectException(EventNameConflictedException::class);

        $registry->init();
    }

    /**
     * Building a Kafka consumer joins the group, so close() must not build one
     * just to leave it.
     */
    public function testClosingAConsumerThatWasNeverBuiltDoesNothing(): void
    {
        $factory = new ConsumerFactory($this->config);

        $factory->close('default', ['topic' => ['user-events.billing-service.dlq'], 'group_id' => 'billing-service-dlq-replay']);

        $this->addToAssertionCount(1);
    }

    /**
     * The dead letters are written before the replay group exists, so on Redis
     * that group must start at the beginning of the stream whatever
     * `read_from` says.
     */
    public function testARedisGroupCanStartAtTheBeginningOfTheStream(): void
    {
        $redis = Mockery::mock(RedisProxy::class);
        $redis->shouldReceive('xinfo')->with('GROUPS', 'user-events.billing-service.dlq')->andReturn(false);
        $redis->shouldReceive('xgroup')->once()->with('CREATE', 'user-events.billing-service.dlq', 'billing-service-dlq-replay', '0', true)->andReturn(true);
        /** @var Mockery\MockInterface&RedisFactory $factory */
        $factory = Mockery::mock(RedisFactory::class);
        $factory->shouldReceive('get')->with('default')->andReturn($redis);

        $stream = new RedisStream($factory, new Serializer(), options: ['read_from' => ReadMessageFrom::GROUP_CREATED]);

        $this->assertTrue($stream->createGroup('billing-service-dlq-replay', 'user-events.billing-service.dlq', ReadMessageFrom::BEGINNING));
    }

    public function testARedisGroupStillFollowsReadFromByDefault(): void
    {
        $redis = Mockery::mock(RedisProxy::class);
        $redis->shouldReceive('xinfo')->andReturn(false);
        $redis->shouldReceive('xgroup')->once()->with('CREATE', 'user-events', 'billing-service', '$', true)->andReturn(true);
        /** @var Mockery\MockInterface&RedisFactory $factory */
        $factory = Mockery::mock(RedisFactory::class);
        $factory->shouldReceive('get')->andReturn($redis);

        $stream = new RedisStream($factory, new Serializer(), options: ['read_from' => ReadMessageFrom::GROUP_CREATED]);

        $this->assertTrue($stream->createGroup('billing-service', 'user-events'));
    }
}
