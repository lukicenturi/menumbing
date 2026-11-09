<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Tests\Cases;

use Closure;
use DateTimeImmutable;
use DomainException;
use Generator;
use Hyperf\Context\Context;
use Hyperf\Di\Annotation\AnnotationCollector;
use InvalidArgumentException;
use Menumbing\Contract\EventStream\StreamMessage;
use Menumbing\EventStream\Annotation\ConsumedEvent;
use Menumbing\EventStream\DeadLetter\DeadLetterOptions;
use Menumbing\EventStream\DeadLetter\DeadLetterStream;
use Menumbing\EventStream\Enum\Result;
use Menumbing\EventStream\Event\AfterConsume;
use Menumbing\EventStream\Event\BeforeConsume;
use Menumbing\EventStream\Event\ConsumeFailed;
use Menumbing\EventStream\Event\DeadLettered;
use Menumbing\EventStream\EventRegistry;
use Menumbing\EventStream\Exception\MessageNackedException;
use Menumbing\EventStream\Handler\ConsumerEventHandler;
use Menumbing\EventStream\Listener\DebugListener;
use Menumbing\EventStream\Tests\Support\SpyStdoutLogger;
use Menumbing\EventStream\Tests\Support\TestableConsumerProcess;
use Menumbing\EventStream\Tests\Support\UserCreated;
use Psr\EventDispatcher\EventDispatcherInterface;
use ReflectionMethod;
use RuntimeException;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Serializer\Normalizer\ObjectNormalizer;
use Symfony\Component\Serializer\Serializer;

use function Hyperf\Coroutine\run;

/**
 * The retry and dead-letter path of the consumer process.
 *
 * On Kafka the driver never redelivers a failed message, so before this path
 * one listener exception was a silent, permanent loss. The guarantees pinned
 * here: a failure is retried in place, a message that still fails is copied
 * to the dead-letter stream, and every message is acked exactly once.
 *
 * @internal
 * @coversNothing
 *
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */
class ConsumerProcessDeadLetterTest extends AbstractTestCase
{
    private const STREAM = 'user-events';

    private const DLQ = 'user-events.billing-service.dlq';

    private const MESSAGE_ID = '3f1c9a52-6d0e-4b7a-9c21-8e5d4f7a1b03';

    public function testTheShippedConfigTurnsTheDeadLetterPathOn(): void
    {
        $options = DeadLetterOptions::fromConfig($this->config);

        $this->assertTrue($options->enabled);
        $this->assertSame(3, $options->maxAttempts);
        $this->assertSame(500, $options->backoffMs);
        $this->assertSame(5000, $options->maxBackoffMs);
        // Scoped to the group: the source is a fan-out stream other groups read too.
        $this->assertSame(self::DLQ, $options->streamFor(self::STREAM, self::GROUP));
    }

    public function testTheDefaultsApplyWithoutAConfigBlock(): void
    {
        $this->config->set('event_stream.consumer.dead_letter', null);

        $this->assertEquals(new DeadLetterOptions(), DeadLetterOptions::fromConfig($this->config));
    }

    public function testASuccessIsAckedOnceAndNothingIsDeadLettered(): void
    {
        $process = $this->process(static fn (): Result => Result::ACK);

        $process->consume($this->message());

        $this->assertSame([self::MESSAGE_ID], $this->stream->ackedIds());
        $this->assertSame([], $this->stream->published);
        $this->assertCount(1, $process->handled);
        $this->assertSame(0, $process->handled[0]->context['retry_count']);
        $this->assertCount(1, $this->events->ofType(AfterConsume::class));
        $this->assertSame([], $this->events->ofType(ConsumeFailed::class));
        $this->assertSame([], $process->pauses);
    }

    public function testTwoFailuresThenASuccessIsRetriedInPlace(): void
    {
        $process = $this->process(static function (int $call): Result {
            if ($call < 3) {
                throw new RuntimeException('Deadlock detected');
            }

            return Result::ACK;
        });

        $process->consume($this->message());

        $this->assertSame([self::MESSAGE_ID], $this->stream->ackedIds());
        $this->assertSame([], $this->stream->published);
        $this->assertCount(2, $this->events->ofType(ConsumeFailed::class));
        $this->assertCount(3, $this->events->ofType(BeforeConsume::class));
        $this->assertSame([0, 1, 2], array_map(
            static fn (StreamMessage $m): int => $m->context['retry_count'],
            $process->handled,
        ));
        // 500 ms, then doubled.
        $this->assertSame([500, 1000], $process->pauses);
    }

    public function testAMessageThatAlwaysFailsIsDeadLetteredAndAcked(): void
    {
        $process = $this->process(static function (): never {
            throw new RuntimeException('Account not found');
        });

        $original = $this->message();
        $process->consume($original);

        $this->assertCount(3, $process->handled);
        $this->assertSame([500, 1000], $process->pauses);
        $this->assertCount(3, $this->events->ofType(ConsumeFailed::class));

        $published = $this->stream->publishedTo(self::DLQ);
        $this->assertCount(1, $published);
        $copy = $published[0];
        $this->assertSame('user.created', $copy->type);
        $this->assertSame($original->data, $copy->data);
        // The producer's own context travels with the copy; the attempt counter does not.
        $this->assertSame('7b3e0f4c-2a91-4d6e-b8f5-0c1d2e3f4a5b', $copy->context['trace_id']);
        $this->assertArrayNotHasKey('retry_count', $copy->context);
        // The driver gives the copy a new id; the source id is in the context.
        $this->assertNotSame(self::MESSAGE_ID, $copy->id);

        $deadLetter = $copy->context['dead_letter'];
        $this->assertSame(self::STREAM, $deadLetter['source_stream']);
        $this->assertSame(self::GROUP, $deadLetter['group']);
        $this->assertSame(self::MESSAGE_ID, $deadLetter['message_id']);
        $this->assertSame(3, $deadLetter['attempts']);
        $this->assertNotFalse(DateTimeImmutable::createFromFormat(DATE_ATOM, $deadLetter['failed_at']));
        $this->assertSame(['class' => RuntimeException::class, 'message' => 'Account not found'], $deadLetter['error']);

        $this->assertSame([self::MESSAGE_ID], $this->stream->ackedIds());

        $deadLettered = $this->events->ofType(DeadLettered::class);
        $this->assertCount(1, $deadLettered);
        $this->assertSame(3, $deadLettered[0]->attempts);
        $this->assertSame(self::STREAM, $deadLettered[0]->streamName);
        $this->assertSame(self::GROUP, $deadLettered[0]->groupName);
        $this->assertSame(self::DLQ, $deadLettered[0]->deadLetterStream);
        $this->assertSame(self::MESSAGE_ID, $deadLettered[0]->message->id);
        $this->assertSame('Account not found', $deadLettered[0]->throwable->getMessage());
        $this->assertSame([], $this->events->ofType(AfterConsume::class));
    }

    public function testASkipListedExceptionIsDeadLetteredWithoutARetry(): void
    {
        $process = $this->process(static function (): never {
            throw new InvalidArgumentException('Malformed userId');
        });

        $process->consume($this->message());

        $this->assertCount(1, $process->handled);
        $this->assertSame([], $process->pauses);
        $copy = $this->stream->publishedTo(self::DLQ)[0];
        $this->assertSame(1, $copy->context['dead_letter']['attempts']);
        $this->assertSame(InvalidArgumentException::class, $copy->context['dead_letter']['error']['class']);
        $this->assertSame([self::MESSAGE_ID], $this->stream->ackedIds());
    }

    /**
     * `#[ConsumedEvent(retries: n)]` makes ConsumerEventHandler answer NACK
     * instead of throwing. That spends the same budget, and max_attempts wins.
     */
    public function testNackSpendsTheSameAttemptBudget(): void
    {
        $process = $this->process(static fn (): Result => Result::NACK);

        $process->consume($this->message());

        $this->assertCount(3, $process->handled);
        $copy = $this->stream->publishedTo(self::DLQ)[0];
        $this->assertSame(3, $copy->context['dead_letter']['attempts']);
        $this->assertSame(MessageNackedException::class, $copy->context['dead_letter']['error']['class']);
        $this->assertSame([self::MESSAGE_ID], $this->stream->ackedIds());
    }

    /**
     * Not acking would not keep the message on Kafka: the ack is a counter
     * per partition, so the failed message would be skipped anyway and a good
     * one redelivered in its place. The critical log line is the copy.
     */
    public function testAFailedDeadLetterPublishIsAckedAndLoggedWithTheFullPayload(): void
    {
        $this->stream->failPublishTo = [self::DLQ];
        $process = $this->process(static function (): never {
            throw new DomainException('Account is closed');
        });

        $process->consume($this->message());

        $this->assertSame([], $this->stream->published);
        $this->assertSame([self::MESSAGE_ID], $this->stream->ackedIds());
        $this->assertSame([], $this->events->ofType(DeadLettered::class));

        $critical = $this->logger->messages('critical');
        $this->assertCount(1, $critical);
        $this->assertStringContainsString('EventStream [Dead-letter publish failed]', $critical[0]);
        $this->assertStringContainsString('publish_error=Broker refused ' . self::DLQ, $critical[0]);
        // The whole message, ready to publish by hand.
        $this->assertStringContainsString('"userId":"0c8a6f2e-5b41-4e3d-9f7a-2d6b1c4e8a90"', $critical[0]);
        $this->assertStringContainsString('"dead_letter":{"source_stream":"user-events"', $critical[0]);
    }

    /**
     * The payload line is written before the ack, so an ack that throws
     * cannot take the last copy of the message with it.
     */
    public function testThePayloadIsLoggedEvenWhenTheAckAfterAFailedPublishThrows(): void
    {
        $this->stream->failPublishTo = [self::DLQ];
        $this->stream->failAck = true;
        $process = $this->process(static function (): never {
            throw new DomainException('Account is closed');
        });

        try {
            $process->consume($this->message());
            $this->fail('The ack failure should surface.');
        } catch (RuntimeException $e) {
            $this->assertSame('Offset commit failed', $e->getMessage());
        }

        $critical = $this->logger->messages('critical');
        $this->assertCount(1, $critical);
        $this->assertStringContainsString('"userId":"0c8a6f2e-5b41-4e3d-9f7a-2d6b1c4e8a90"', $critical[0]);
    }

    public function testDisabledFallsBackToThePreviousBehaviour(): void
    {
        $this->config->set('event_stream.consumer.dead_letter.enabled', false);
        $process = $this->process(static function (): never {
            throw new RuntimeException('Account not found');
        });

        $process->consume($this->message());

        // One attempt, no ack, no copy: exactly what the process did before.
        $this->assertCount(1, $process->handled);
        $this->assertSame([], $process->pauses);
        $this->assertSame([], $this->stream->published);
        $this->assertSame([], $this->stream->ackedIds());
        $this->assertCount(1, $this->events->ofType(ConsumeFailed::class));
    }

    public function testDisabledStillAcksAndDropsASkipListedException(): void
    {
        $this->config->set('event_stream.consumer.dead_letter.enabled', false);
        $process = $this->process(static function (): never {
            throw new InvalidArgumentException('Malformed userId');
        });

        $process->consume($this->message());

        $this->assertSame([self::MESSAGE_ID], $this->stream->ackedIds());
        $this->assertSame([], $this->stream->published);
    }

    public function testTheBackoffDoublesAndIsCapped(): void
    {
        $options = new DeadLetterOptions(maxAttempts: 5, backoffMs: 3000, maxBackoffMs: 5000);

        $this->assertSame([3000, 5000, 5000, 5000], array_map($options->backoffAfter(...), [1, 2, 3, 4]));
        $this->assertSame(5000, $options->backoffAfter(1000));
    }

    public function testTheConfiguredBackoffCapIsUsed(): void
    {
        $this->config->set('event_stream.consumer.dead_letter.max_attempts', 4);
        $this->config->set('event_stream.consumer.dead_letter.backoff_ms', 100);
        $this->config->set('event_stream.consumer.dead_letter.max_backoff_ms', 250);
        $process = $this->process(static fn (): Result => Result::NACK);

        $process->consume($this->message());

        $this->assertSame([100, 200, 250], $process->pauses);
    }

    public function testAnEmptySuffixCannotPointTheDeadLetterStreamAtItsSource(): void
    {
        $this->config->set('event_stream.consumer.dead_letter.suffix', '');

        $this->assertSame(self::DLQ, DeadLetterOptions::fromConfig($this->config)->streamFor(self::STREAM, self::GROUP));
        $this->assertSame(self::DLQ, DeadLetterStream::nameFor(self::STREAM, self::GROUP, ''));
    }

    /**
     * With `concurrent` > 1 every message runs in its own coroutine. All retry
     * state is local, so each message gets its own budget and one ack.
     */
    public function testAConcurrentBatchRetriesAndAcksEachMessageOnce(): void
    {
        $failures = [];
        $process = $this->process(static function (int $call, StreamMessage $message) use (&$failures): Result {
            $failures[$message->id] = ($failures[$message->id] ?? 0) + 1;

            // `a` succeeds at once, `b` on its second attempt, `c` never.
            return match ($message->id) {
                'a' => Result::ACK,
                'b' => $failures['b'] < 2 ? Result::NACK : Result::ACK,
                default => throw new RuntimeException('Account not found'),
            };
        });
        $batch = (static function (): Generator {
            foreach (['a', 'b', 'c'] as $id) {
                yield new StreamMessage(self::STREAM, 'user.created', ['userId' => $id], [], $id);
            }
        })();

        run(static function () use ($process, $batch): void {
            (new ReflectionMethod($process, 'processBatch'))->invoke($process, $batch);
        });

        $acked = $this->stream->ackedIds();
        sort($acked);
        $this->assertSame(['a', 'b', 'c'], $acked);
        $this->assertSame(['a' => 1, 'b' => 2, 'c' => 3], $failures);
        $dead = $this->stream->publishedTo(self::DLQ);
        $this->assertCount(1, $dead);
        $this->assertSame('c', $dead[0]->context['dead_letter']['message_id']);
    }

    /**
     * A listener of DeadLettered that throws must not leave the original
     * unacked: that would shift the Kafka counter by one.
     */
    public function testAThrowingDeadLetteredListenerStillLetsTheOriginalBeAcked(): void
    {
        $this->container->set(EventDispatcherInterface::class, new class implements EventDispatcherInterface {
            public function dispatch(object $event): object
            {
                if ($event instanceof DeadLettered) {
                    throw new RuntimeException('Listener broke');
                }

                return $event;
            }
        });
        $process = $this->process(static fn (): Result => Result::NACK);

        try {
            $process->consume($this->message());
            $this->fail('The listener failure should surface.');
        } catch (RuntimeException $e) {
            $this->assertSame('Listener broke', $e->getMessage());
        }

        $this->assertCount(1, $this->stream->publishedTo(self::DLQ));
        $this->assertSame([self::MESSAGE_ID], $this->stream->ackedIds());
    }

    public function testTheDeadLetteredLineIsLoggedAsAnErrorEvenWithDebugOff(): void
    {
        $this->config->set('event_stream.debug', false);
        $logger = new SpyStdoutLogger();
        $listener = new DebugListener($logger, $this->config);

        $listener->process(new DeadLettered(
            'user-events-pod-1',
            self::GROUP,
            self::STREAM,
            self::DLQ,
            $this->message(),
            3,
            new RuntimeException('Account not found'),
        ));

        $this->assertSame(
            ['EventStream [Dead-lettered] consumer=user-events-pod-1 group=billing-service stream=user-events dead_letter_stream=' . self::DLQ . ' message_id=' . self::MESSAGE_ID . ' type=user.created attempts=3 error=Account not found'],
            $logger->messages('error'),
        );
    }

    public function testTheDebugListenerListensToDeadLettered(): void
    {
        $this->assertContains(DeadLettered::class, (new DebugListener($this->logger, $this->config))->listen());
    }

    /**
     * `retries: n` with n >= max_attempts: every attempt answers NACK and
     * none rethrows. The real error must still reach ConsumeFailed and the
     * dead letter, not a bare "handler returned NACK".
     */
    public function testARetriesBudgetAboveMaxAttemptsKeepsTheRealError(): void
    {
        AnnotationCollector::collectClass(UserCreated::class, ConsumedEvent::class, new ConsumedEvent(stream: self::STREAM, name: 'user.created', retries: 5));
        $registry = new EventRegistry();
        $registry->init();
        $listeners = new class implements EventDispatcherInterface {
            public function dispatch(object $event): object
            {
                throw new RuntimeException('Connection to the ledger timed out');
            }
        };
        $handler = new ConsumerEventHandler($listeners, $registry, new Serializer([new ObjectNormalizer()], [new JsonEncoder()]));
        $process = $this->process(static fn (int $call, StreamMessage $message): Result => $handler(self::GROUP, $message));

        $process->consume($this->message());

        $this->assertCount(3, $process->handled);
        $failed = $this->events->ofType(ConsumeFailed::class);
        $this->assertCount(3, $failed);
        foreach ($failed as $event) {
            $this->assertSame('Connection to the ledger timed out', $event->exception->getMessage());
        }
        $error = $this->stream->publishedTo(self::DLQ)[0]->context['dead_letter']['error'];
        $this->assertSame(['class' => RuntimeException::class, 'message' => 'Connection to the ledger timed out'], $error);
        $this->assertSame([self::MESSAGE_ID], $this->stream->ackedIds());
        $this->assertNull(ConsumerEventHandler::takeNackReason());
    }

    public function testAStaleNackReasonIsNotTakenForALaterPlainNack(): void
    {
        Context::set(ConsumerEventHandler::NACK_REASON, new RuntimeException('Stale'));
        $process = $this->process(static fn (): Result => Result::NACK);

        $process->consume($this->message());

        $this->assertSame(MessageNackedException::class, $this->stream->publishedTo(self::DLQ)[0]->context['dead_letter']['error']['class']);
    }

    /**
     * The disabled path keeps its counter: a NACK leaves the message unacked,
     * and the next delivery of the same id carries the next retry_count.
     */
    public function testDisabledKeepsCountingNacksPerMessage(): void
    {
        $this->config->set('event_stream.consumer.dead_letter.enabled', false);
        $process = $this->process(static fn (int $call): Result => $call < 3 ? Result::NACK : Result::ACK);

        $process->consume($this->message());
        $process->consume($this->message());
        $this->assertSame([], $this->stream->ackedIds());

        $process->consume($this->message());

        $this->assertSame([0, 1, 2], array_map(static fn (StreamMessage $m): int => $m->context['retry_count'], $process->handled));
        $this->assertSame([self::MESSAGE_ID], $this->stream->ackedIds());
        $this->assertSame([], $this->stream->published);
        $this->assertSame([], $process->pauses);
    }

    /**
     * When the DeadLettered listener throws and then the ack throws as well,
     * the error line about the listener exists already: the ack failure
     * cannot hide where the message went.
     */
    public function testAThrowingDeadLetteredListenerIsLoggedEvenWhenTheAckThrows(): void
    {
        $this->container->set(EventDispatcherInterface::class, new class implements EventDispatcherInterface {
            public function dispatch(object $event): object
            {
                if ($event instanceof DeadLettered) {
                    throw new RuntimeException('Listener broke');
                }

                return $event;
            }
        });
        $this->stream->failAck = true;
        $process = $this->process(static fn (): Result => Result::NACK);

        try {
            $process->consume($this->message());
            $this->fail('The ack failure should surface.');
        } catch (RuntimeException $e) {
            $this->assertSame('Offset commit failed', $e->getMessage());
        }

        $this->assertCount(1, $this->stream->publishedTo(self::DLQ));
        $errors = $this->logger->messages('error');
        $this->assertCount(1, $errors);
        $this->assertStringContainsString('EventStream [Dead-lettered listener failed]', $errors[0]);
        $this->assertStringContainsString('dead_letter_stream=' . self::DLQ . ' message_id=' . self::MESSAGE_ID, $errors[0]);
        $this->assertStringContainsString('error=Listener broke', $errors[0]);
    }

    /**
     * RedisStream adds `owner` to an entry it claims back; that describes the
     * delivery, not the message, so the dead letter does not carry it.
     */
    public function testTheRedisOwnerIsNotCopiedToTheDeadLetter(): void
    {
        $process = $this->process(static fn (): Result => Result::NACK);

        $process->consume($this->message()->withContext(['owner' => 'consumer-that-crashed']));

        $copy = $this->stream->publishedTo(self::DLQ)[0];
        $this->assertArrayNotHasKey('owner', $copy->context);
        $this->assertSame('7b3e0f4c-2a91-4d6e-b8f5-0c1d2e3f4a5b', $copy->context['trace_id']);
    }

    private function process(Closure $script): TestableConsumerProcess
    {
        return new TestableConsumerProcess($this->container, $script, self::STREAM, self::GROUP);
    }

    private function message(): StreamMessage
    {
        return new StreamMessage(
            stream: self::STREAM,
            type: 'user.created',
            data: [
                'userId' => '0c8a6f2e-5b41-4e3d-9f7a-2d6b1c4e8a90',
                'email' => 'jane@example.com',
            ],
            context: ['trace_id' => '7b3e0f4c-2a91-4d6e-b8f5-0c1d2e3f4a5b', 'retry_count' => 0],
            id: self::MESSAGE_ID,
        );
    }
}
