<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Tests\Cases;

use Hyperf\Di\Annotation\AnnotationCollector;
use Hyperf\Redis\RedisFactory;
use Hyperf\Redis\RedisProxy;
use longlang\phpkafka\Broker;
use longlang\phpkafka\Client\ClientInterface;
use longlang\phpkafka\Consumer\Consumer;
use longlang\phpkafka\Consumer\ConsumerConfig;
use longlang\phpkafka\Consumer\OffsetManager;
use longlang\phpkafka\Protocol\ErrorCode;
use longlang\phpkafka\Protocol\ListOffset\ListOffsetPartitionResponse;
use longlang\phpkafka\Protocol\ListOffset\ListOffsetRequest;
use longlang\phpkafka\Protocol\ListOffset\ListOffsetResponse;
use longlang\phpkafka\Protocol\ListOffset\ListOffsetTopicResponse;
use Menumbing\EventStream\Annotation\ConsumedEvent;
use Menumbing\EventStream\Driver\Kafka\ConsumerFactory;
use Menumbing\EventStream\Driver\Kafka\EarliestOffsetConsumer;
use Menumbing\EventStream\Driver\Redis\RedisStream;
use Menumbing\EventStream\Enum\ReadMessageFrom;
use Menumbing\EventStream\EventRegistry;
use Menumbing\EventStream\Exception\EventNameConflictedException;
use Menumbing\EventStream\Tests\Support\RecordingEarliestOffsetConsumer;
use Menumbing\EventStream\Tests\Support\UserCreated;
use Mockery;
use RuntimeException;
use ReflectionClass;
use ReflectionMethod;
use ReflectionProperty;
use Symfony\Component\Serializer\Serializer;

/**
 * The small additions the replay command relies on outside the command:
 * the registry, the Kafka consumer factory and earliest-offset consumer, and
 * the Redis group read position.
 *
 * @internal
 * @coversNothing
 *
 * @author  Luki Centuri <lukicenturi@gmail.com>
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

    public function testOffsetResetEarliestSelectsTheEarliestOffsetConsumer(): void
    {
        $factory = new ConsumerFactory($this->config);
        $consumerClass = new ReflectionMethod($factory, 'getConsumerClass');

        $this->assertSame(EarliestOffsetConsumer::class, $consumerClass->invoke($factory, ['offset_reset' => 'earliest']));
        $this->assertSame(Consumer::class, $consumerClass->invoke($factory, []));
        $this->assertSame(Consumer::class, $consumerClass->invoke($factory, ['offset_reset' => 'latest']));
    }

    public function testOnlyPositionsBelowTheEarliestRetainedOffsetMove(): void
    {
        $this->assertSame(
            [0 => 40],
            EarliestOffsetConsumer::stepsToEarliest([0 => 0, 1 => 7, 2 => 3], [0 => 40, 1 => 5]),
        );
    }

    /**
     * longlang/phpkafka starts a new group at offset 0 and, when that offset
     * is not retained, jumps to the newest message. After a (re)join this
     * consumer asks for the EARLIEST offsets (timestamp -2) and moves a
     * position that is not retained up to it; a retained one stays.
     */
    public function testTheConsumerMovesANotRetainedPositionToTheEarliestOffset(): void
    {
        $requests = [];
        $client = Mockery::mock(ClientInterface::class);
        $client->shouldReceive('sendRecv')->andReturnUsing(static function (ListOffsetRequest $request) use (&$requests): ListOffsetResponse {
            $requests[] = $request;

            return (new ListOffsetResponse())->setTopics([
                (new ListOffsetTopicResponse())->setName('user-events.billing-service.dlq')->setPartitions([
                    (new ListOffsetPartitionResponse())->setPartitionIndex(0)->setErrorCode(ErrorCode::NONE)->setOffset(40),
                    (new ListOffsetPartitionResponse())->setPartitionIndex(1)->setErrorCode(ErrorCode::NONE)->setOffset(5),
                ]),
            ]);
        });
        /** @var Broker&Mockery\MockInterface $broker */
        $broker = Mockery::mock(Broker::class);
        $broker->shouldReceive('getTopicsMeta')->andReturn([]);
        $broker->shouldReceive('getBrokerIdByTopic')->andReturn(1);
        $broker->shouldReceive('getClientByBrokerId')->with(1)->andReturn($client);

        $offsetManager = new OffsetManager($broker, 1, 'user-events.billing-service.dlq', [0, 1], 'billing-service-dlq-replay', null, 'member-1', 1);
        (new ReflectionProperty(OffsetManager::class, 'offsets'))->setValue($offsetManager, [0 => 0, 1 => 7]);

        // Built without the constructor, which would connect to a broker.
        $consumer = (new ReflectionClass(EarliestOffsetConsumer::class))->newInstanceWithoutConstructor();
        (new ReflectionProperty(Consumer::class, 'broker'))->setValue($consumer, $broker);
        (new ReflectionProperty(Consumer::class, 'config'))->setValue($consumer, new ConsumerConfig());
        (new ReflectionProperty(Consumer::class, 'offsetManagers'))->setValue($consumer, ['user-events.billing-service.dlq' => $offsetManager]);

        (new ReflectionMethod($consumer, 'seekToEarliestWhereNotRetained'))->invoke($consumer);

        $this->assertSame([0 => 40, 1 => 7], $offsetManager->getOffsets());
        $this->assertCount(1, $requests);
        $partitions = $requests[0]->getTopics()[0]->getPartitions();
        $this->assertSame([EarliestOffsetConsumer::EARLIEST_TIMESTAMP, EarliestOffsetConsumer::EARLIEST_TIMESTAMP], array_map(static fn ($p): int => $p->getTimestamp(), $partitions));
    }

    public function testRejoinJoinsFirstAndThenSeeks(): void
    {
        $consumer = (new ReflectionClass(RecordingEarliestOffsetConsumer::class))->newInstanceWithoutConstructor();

        $consumer->rejoin();

        $this->assertSame(['join', 'seek'], $consumer->calls);
    }

    /**
     * The join started the heartbeat; a failed seek must not leave a joined
     * member with a live timer. It is rethrown, not swallowed: falling back to
     * the library position could jump to the newest message.
     */
    public function testAFailedSeekStopsTheHeartbeatLeavesTheGroupAndRethrows(): void
    {
        $consumer = (new ReflectionClass(RecordingEarliestOffsetConsumer::class))->newInstanceWithoutConstructor();
        $consumer->failSeek = true;

        try {
            $consumer->rejoin();
            $this->fail('The seek failure should surface.');
        } catch (RuntimeException $e) {
            $this->assertSame('ListOffsets timed out', $e->getMessage());
        }

        $this->assertSame(['join', 'seek', 'stop-heartbeat', 'close'], $consumer->calls);
    }

    /**
     * `getBrokerIdByTopic()` gives the metadata leader id, -1 for a partition
     * without a leader. That partition keeps its position; the others seek.
     */
    public function testAPartitionWithoutALeaderIsSkipped(): void
    {
        $requests = [];
        $client = Mockery::mock(ClientInterface::class);
        $client->shouldReceive('sendRecv')->andReturnUsing(static function (ListOffsetRequest $request) use (&$requests): ListOffsetResponse {
            $requests[] = $request;

            return (new ListOffsetResponse())->setTopics([
                (new ListOffsetTopicResponse())->setName('user-events.billing-service.dlq')->setPartitions([
                    (new ListOffsetPartitionResponse())->setPartitionIndex(0)->setErrorCode(ErrorCode::NONE)->setOffset(40),
                ]),
            ]);
        });
        /** @var Broker&Mockery\MockInterface $broker */
        $broker = Mockery::mock(Broker::class);
        $broker->shouldReceive('getTopicsMeta')->andReturn([]);
        $broker->shouldReceive('getBrokerIdByTopic')->with('user-events.billing-service.dlq', 0)->andReturn(1);
        $broker->shouldReceive('getBrokerIdByTopic')->with('user-events.billing-service.dlq', 1)->andReturn(-1);
        $broker->shouldReceive('getClientByBrokerId')->with(1)->andReturn($client);
        $broker->shouldNotReceive('getClientByBrokerId')->with(-1);

        $offsetManager = new OffsetManager($broker, 1, 'user-events.billing-service.dlq', [0, 1], 'billing-service-dlq-replay', null, 'member-1', 1);
        (new ReflectionProperty(OffsetManager::class, 'offsets'))->setValue($offsetManager, [0 => 0, 1 => 0]);
        $consumer = (new ReflectionClass(EarliestOffsetConsumer::class))->newInstanceWithoutConstructor();
        (new ReflectionProperty(Consumer::class, 'broker'))->setValue($consumer, $broker);
        (new ReflectionProperty(Consumer::class, 'config'))->setValue($consumer, new ConsumerConfig());
        (new ReflectionProperty(Consumer::class, 'offsetManagers'))->setValue($consumer, ['user-events.billing-service.dlq' => $offsetManager]);

        (new ReflectionMethod($consumer, 'seekToEarliestWhereNotRetained'))->invoke($consumer);

        $this->assertSame([0 => 40, 1 => 0], $offsetManager->getOffsets());
        $this->assertCount(1, $requests);
        $this->assertSame([0], array_map(static fn ($p): int => $p->getPartitionIndex(), $requests[0]->getTopics()[0]->getPartitions()));
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
