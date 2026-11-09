<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Tests\Cases;

use Generator;
use Hyperf\Context\Context;
use Hyperf\Di\Annotation\AnnotationCollector;
use Hyperf\Event\ListenerProvider;
use Hyperf\Process\ProcessManager;
use Menumbing\Contract\EventStream\StreamMessage;
use Menumbing\EventStream\Annotation\ConsumedEvent;
use Menumbing\EventStream\Command\DlqReplayCommand;
use Menumbing\EventStream\ConfigProvider;
use Menumbing\EventStream\Driver\Kafka\KafkaStream;
use Menumbing\EventStream\Enum\Result;
use Menumbing\EventStream\Exception\MessageNackedException;
use Menumbing\EventStream\Factory\StreamFactory;
use Menumbing\EventStream\Handler\ConsumerEventHandler;
use Menumbing\EventStream\Tests\Support\TestableDlqReplayCommand;
use Menumbing\EventStream\Tests\Support\UserCreated;
use Mockery;
use Psr\EventDispatcher\ListenerProviderInterface;
use ReflectionProperty;
use RuntimeException;
use Symfony\Component\Console\Input\StringInput;
use Symfony\Component\Console\Output\BufferedOutput;

/**
 * @internal
 * @coversNothing
 *
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */
class DlqReplayCommandTest extends AbstractTestCase
{
    private const SOURCE = 'user-events';

    private const DLQ = 'user-events.billing-service.dlq';

    private const REPLAY_GROUP = 'billing-service-dlq-replay';

    private const TYPE = 'user.created';

    private BufferedOutput $output;

    private TestableDlqReplayCommand $command;

    protected function tearDown(): void
    {
        Mockery::close();

        parent::tearDown();
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->output = new BufferedOutput();
        $this->command = new TestableDlqReplayCommand($this->container, $this->config);
        $this->command->script = static fn (): Result => Result::ACK;
    }

    public function testTheCommandIsRegistered(): void
    {
        $this->assertContains(DlqReplayCommand::class, (new ConfigProvider())()['commands']);
        $this->assertSame(DlqReplayCommand::COMMAND_SIGNATURE, $this->command->getName());
    }

    public function testEveryDeadLetteredMessageIsReplayedLocallyAndAcked(): void
    {
        $first = $this->deadLetter('dlq-1', 'source-1');
        $second = $this->deadLetter('dlq-2', 'source-2');
        $third = $this->deadLetter('dlq-3', 'source-3');

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --idle-seconds=0'));

        $this->assertSame([], $this->stream->published);
        $this->assertCount(3, $this->command->dispatched);
        $this->assertSame([], $this->stream->queues[self::DLQ]);
        $this->assertSame([$first->id, $second->id, $third->id], $this->stream->ackedIds());
        $this->assertSame([self::REPLAY_GROUP], array_values(array_unique(array_column($this->stream->acked, 'group'))));
        $this->assertSame([['name' => self::REPLAY_GROUP, 'stream' => self::DLQ]], $this->stream->groups);

        $replayed = $this->command->dispatched[0];
        $this->assertSame(self::SOURCE, $replayed->stream);
        $this->assertSame(self::TYPE, $replayed->type);
        $this->assertSame($first->data, $replayed->data);
        $this->assertArrayNotHasKey('dead_letter', $replayed->context);
        $this->assertSame(0, $replayed->context['retry_count']);
        $this->assertSame('7b3e0f4c-2a91-4d6e-b8f5-0c1d2e3f4a5b', $replayed->context['trace_id']);
        $this->assertSame('source-1', $replayed->context['replayed_from_dlq']['original_message_id']);
        $this->assertSame(1, $replayed->context['replayed_from_dlq']['count']);

        $this->assertContains('boot', $this->command->calls);

        $text = $this->output->fetch();
        $this->assertSame(3, substr_count($text, 'REPLAYED dlq_id='));
        $this->assertMatchesRegularExpression('/Replayed\s*\|?\s*3/', $text);
    }

    public function testAReplayThatFailsAgainGoesBackToTheDeadLetterStreamCounted(): void
    {
        $message = $this->deadLetter('dlq-1', 'source-1');
        $this->command->script = static function (): never {
            throw new RuntimeException('Account still missing');
        };

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --idle-seconds=30'));

        $this->assertSame([], $this->stream->publishedTo(self::SOURCE));
        $this->assertSame([$message->id], $this->stream->ackedIds());
        $left = $this->stream->queues[self::DLQ];
        $this->assertCount(1, $left);
        $this->assertSame(1, $left[0]->context['replayed_from_dlq']['count']);
        $this->assertSame('source-1', $left[0]->context['replayed_from_dlq']['original_message_id']);
        $this->assertSame('Account not found', $left[0]->context['dead_letter']['error']['message']);
        $this->assertSame('Account still missing', $left[0]->context['dead_letter']['last_replay_error']['message']);
        $this->assertSame(RuntimeException::class, $left[0]->context['dead_letter']['last_replay_error']['class']);
        $this->assertArrayNotHasKey('retry_count', $left[0]->context);
        $this->assertCount(1, $this->command->dispatched);
        $text = $this->output->fetch();
        $this->assertMatchesRegularExpression('/Failed again \(kept on the DLQ\)\s*\|?\s*1/', $text);
        $this->assertStringContainsString('every new message was read', $text);
    }

    public function testANackCountsAsAFailedReplay(): void
    {
        $this->deadLetter('dlq-1', 'source-1');
        $this->command->script = static fn (): Result => Result::NACK;

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --idle-seconds=30'));

        $left = $this->stream->queues[self::DLQ];
        $this->assertCount(1, $left);
        $this->assertSame(MessageNackedException::class, $left[0]->context['dead_letter']['last_replay_error']['class']);
    }

    public function testTheLimitStopsTheRun(): void
    {
        $this->deadLetter('dlq-1', 'source-1');
        $this->deadLetter('dlq-2', 'source-2');
        $third = $this->deadLetter('dlq-3', 'source-3');

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --limit=2 --idle-seconds=0'));

        $this->assertCount(2, $this->command->dispatched);
        $this->assertSame([$third->id], $this->ids($this->stream->queues[self::DLQ]));
        $this->assertStringContainsString('limit reached (2)', $this->output->fetch());
    }

    public function testADryRunDispatchesNothingAcksNothingAndStillEnds(): void
    {
        $this->deadLetter('dlq-1', 'source-1');
        $this->deadLetter('dlq-2', 'source-2');

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --dry-run --idle-seconds=30'));

        $this->assertSame([], $this->command->dispatched);
        $this->assertNotContains('boot', $this->command->calls);
        $this->assertSame([], $this->stream->published);
        $this->assertSame([], $this->stream->acked);
        $this->assertCount(2, $this->stream->queues[self::DLQ]);
        $this->assertCount(2, $this->stream->subscriptions);

        $text = $this->output->fetch();
        $this->assertStringContainsString('=== DRY-RUN ===', $text);
        $this->assertSame(2, substr_count($text, 'WOULD REPLAY dlq_id='));
    }

    public function testTheTypeFilterReplaysMatchesAndKeepsTheRestOnTheDeadLetterStream(): void
    {
        $match = $this->deadLetter('dlq-1', 'source-1');
        $other = $this->deadLetter('dlq-2', 'source-2', 'user.deleted');

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --type=' . self::TYPE . ' --idle-seconds=30'));

        $this->assertCount(1, $this->command->dispatched);
        $this->assertSame($match->type, $this->command->dispatched[0]->type);

        $left = $this->stream->queues[self::DLQ];
        $this->assertCount(1, $left);
        $this->assertSame('user.deleted', $left[0]->type);
        $this->assertSame($other->context['dead_letter'], $left[0]->context['dead_letter']);
        $this->assertSame([$match->id, $other->id], $this->stream->ackedIds());
        $this->assertMatchesRegularExpression('/Skipped \(--type filter\)\s*\|?\s*1/', $this->output->fetch());
    }

    public function testAFailedReParkStopsTheRunWithoutAckingIt(): void
    {
        $this->deadLetter('dlq-2', 'source-2', 'user.deleted');
        $this->deadLetter('dlq-1', 'source-1');
        $this->stream->failPublishTo = [self::DLQ];

        $this->assertSame(1, $this->replay('--stream=' . self::SOURCE . ' --type=' . self::TYPE . ' --idle-seconds=0'));

        $this->assertSame([], $this->stream->acked);
        $this->assertSame([], $this->command->dispatched);
        $this->assertCount(2, $this->stream->queues[self::DLQ]);
        $text = $this->output->fetch();
        $this->assertStringContainsString('COULD NOT RE-PARK dlq_id=dlq-2', $text);
        $this->assertStringContainsString('re-park to the dead-letter stream failed', $text);
    }

    public function testAnotherGroupsDeadLetterIsKeptNotProcessed(): void
    {
        $foreign = $this->deadLetter('dlq-1', 'source-1', group: 'audit-service');
        $this->deadLetter('dlq-2', 'source-2');

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --idle-seconds=30'));

        $this->assertCount(1, $this->command->dispatched);
        $left = $this->stream->queues[self::DLQ];
        $this->assertCount(1, $left);
        $this->assertSame('audit-service', $left[0]->context['dead_letter']['group']);
        $this->assertSame($foreign->context['dead_letter'], $left[0]->context['dead_letter']);
        $this->assertMatchesRegularExpression('/Skipped \(other group\)\s*\|?\s*1/', $this->output->fetch());
    }

    public function testAnUnknownTypeIsKeptInsteadOfBeingAckedAway(): void
    {
        $this->command->unknownTypes = ['user.retired-event'];
        $unknown = $this->deadLetter('dlq-1', 'source-1', 'user.retired-event');

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --idle-seconds=30'));

        $this->assertSame([], $this->command->dispatched);
        $left = $this->stream->queues[self::DLQ];
        $this->assertCount(1, $left);
        $this->assertSame('user.retired-event', $left[0]->type);
        $this->assertStringContainsString('UNKNOWN TYPE dlq_id=' . $unknown->id, $this->output->fetch());
    }

    public function testThePoisonGuardHoldsAMessageReplayedTooOften(): void
    {
        $this->deadLetter('dlq-1', 'source-1', replays: 3);
        $this->deadLetter('dlq-2', 'source-2', replays: 2);

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --idle-seconds=30'));

        $this->assertCount(1, $this->command->dispatched);
        $this->assertSame(3, $this->command->dispatched[0]->context['replayed_from_dlq']['count']);

        $left = $this->stream->queues[self::DLQ];
        $this->assertCount(1, $left);
        $this->assertSame('source-1', $left[0]->context['replayed_from_dlq']['original_message_id']);
        $this->assertSame(3, $left[0]->context['replayed_from_dlq']['count']);

        $text = $this->output->fetch();
        $this->assertStringContainsString('HELD (replayed 3 times) dlq_id=dlq-1', $text);
        $this->assertStringContainsString('"userId":"0c8a6f2e-5b41-4e3d-9f7a-2d6b1c4e8a90"', $text);
    }

    public function testDiscardPoisonedAcksTheMessageAndLogsItsPayload(): void
    {
        $poison = $this->deadLetter('dlq-1', 'source-1', replays: 3);

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --discard-poisoned --idle-seconds=0'));

        $this->assertSame([], $this->stream->published);
        $this->assertSame([$poison->id], $this->stream->ackedIds());
        $this->assertSame([], $this->stream->queues[self::DLQ]);

        $warnings = $this->logger->messages('warning');
        $this->assertCount(1, $warnings);
        $this->assertStringContainsString('EventStream [Dead-letter discarded]', $warnings[0]);
        $this->assertStringContainsString('"userId":"0c8a6f2e-5b41-4e3d-9f7a-2d6b1c4e8a90"', $warnings[0]);
    }

    public function testTheStopIsKeyedOnTheLineageNotTheMessageId(): void
    {
        $this->deadLetter('dlq-1', 'source-1');
        $this->deadLetter('dlq-1-copy', 'source-1');

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --idle-seconds=30'));

        $this->assertCount(1, $this->command->dispatched);
        $this->assertStringContainsString('every new message was read', $this->output->fetch());
    }

    public function testAMessageWithoutLineageIsReParkedOnceAndTheRunEnds(): void
    {
        $this->stream->push(new StreamMessage(self::DLQ, 'user.deleted', ['userId' => 'u-1'], [], 'hand-made'));

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --type=' . self::TYPE . ' --idle-seconds=30'));

        $left = $this->stream->queues[self::DLQ];
        $this->assertCount(1, $left);
        $this->assertSame('hand-made', $left[0]->context['dead_letter']['message_id']);
        $this->assertCount(1, $this->stream->published);
    }

    public function testAReadFailureStillLeavesTheGroupAndClearsTimers(): void
    {
        $this->stream->failFirstSubscribe = true;

        $this->assertSame(1, $this->replay('--stream=' . self::SOURCE . ' --idle-seconds=0'));

        $this->assertSame(['leave:' . self::DLQ, 'clear-timers'], $this->command->calls);
        $this->assertStringContainsString('Could not read ' . self::DLQ . ': Connection refused', $this->output->fetch());
    }

    public function testAnUnknownDriverFailsWithoutLeavingAGroupButStillClearsTimers(): void
    {
        $this->assertSame(1, $this->replay('--stream=' . self::SOURCE . ' --driver=missing --idle-seconds=0'));

        $this->assertSame(['clear-timers'], $this->command->calls);
        $this->assertSame([], $this->stream->subscriptions);
        $this->assertStringContainsString('Could not read ' . self::DLQ, $this->output->fetch());
    }

    public function testTheStreamOptionIsRequiredAndMustNameTheSource(): void
    {
        $this->assertSame(1, $this->replay('--idle-seconds=0'));
        $this->assertSame(1, $this->replay('--stream=' . self::DLQ . ' --idle-seconds=0'));
        $this->assertSame([], $this->stream->subscriptions);
    }

    public function testAnEmptyDeadLetterStreamEndsOnTheIdleTimeout(): void
    {
        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --idle-seconds=0'));

        $this->assertSame([['group' => self::REPLAY_GROUP, 'stream' => self::DLQ]], $this->stream->subscriptions);
        $this->assertSame(['leave:' . self::DLQ, 'clear-timers'], $this->command->calls);
        $this->assertStringContainsString('no new message for', $this->output->fetch());
    }

    public function testTheCommandRefusesToRunInsideAServerProcess(): void
    {
        $this->deadLetter('dlq-1', 'source-1');
        ProcessManager::setRunning(true);

        $this->assertSame(1, $this->replay('--stream=' . self::SOURCE . ' --idle-seconds=0'));

        $this->assertSame([], $this->stream->subscriptions);
        $this->assertSame([], $this->command->dispatched);
        $this->assertStringContainsString('CLI-only', $this->output->fetch());
    }

    public function testPendingEntriesOfAnEarlierRunAreClaimedFirst(): void
    {
        $pending = $this->deadLetter('dlq-1', 'source-1');
        $this->stream->queues[self::DLQ] = [];
        $this->stream->pending[self::DLQ] = [$pending];
        $fresh = $this->deadLetter('dlq-2', 'source-2');

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --idle-seconds=0'));

        $this->assertSame([$pending->id, $fresh->id], $this->stream->ackedIds());
        $this->assertSame([], $this->stream->pending[self::DLQ]);
        $this->assertSame(['source-1', 'source-2'], array_map(
            static fn (StreamMessage $m): string => $m->context['replayed_from_dlq']['original_message_id'],
            $this->command->dispatched,
        ));
    }

    public function testADryRunEndsThePendingPhaseOnTheFirstRepeat(): void
    {
        $pending = $this->deadLetter('dlq-1', 'source-1');
        $this->stream->queues[self::DLQ] = [];
        $this->stream->pending[self::DLQ] = [$pending];
        $this->deadLetter('dlq-2', 'source-2');

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --dry-run --idle-seconds=30'));

        $this->assertSame([], $this->stream->acked);
        $this->assertSame(2, substr_count($this->output->fetch(), 'WOULD REPLAY dlq_id='));
    }

    public function testTheRealCommandDispatchesTheConsumedEventClassLocally(): void
    {
        AnnotationCollector::collectClass(UserCreated::class, ConsumedEvent::class, new ConsumedEvent(stream: self::SOURCE, name: self::TYPE));
        $this->container->set(ListenerProviderInterface::class, new ListenerProvider());
        $message = $this->deadLetter('dlq-1', 'source-1');
        $this->deadLetter('dlq-2', 'source-2', 'user.retired-event');
        $command = new class($this->container, $this->config) extends DlqReplayCommand {
            protected function clearTimers(): void
            {
            }
        };

        $this->assertSame(0, $command->run(new StringInput('--stream=' . self::SOURCE . ' --idle-seconds=30 --disable-event-dispatcher'), $this->output));

        $dispatched = $this->events->ofType(UserCreated::class);
        $this->assertCount(1, $dispatched);
        $this->assertSame('0c8a6f2e-5b41-4e3d-9f7a-2d6b1c4e8a90', $dispatched[0]->userId);
        $this->assertSame('jane@example.com', $dispatched[0]->email);
        $this->assertSame([$message->id, 'dlq-2'], $this->stream->ackedIds());
        $this->assertSame(['user.retired-event'], array_map(static fn (StreamMessage $m): string => $m->type, $this->stream->queues[self::DLQ]));
    }

    public function testAReplayNackRecordsTheRealError(): void
    {
        $this->deadLetter('dlq-1', 'source-1');
        $this->command->script = static function (): Result {
            Context::set(ConsumerEventHandler::NACK_REASON, new RuntimeException('Ledger is down'));

            return Result::NACK;
        };

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --idle-seconds=30'));

        $error = $this->stream->queues[self::DLQ][0]->context['dead_letter']['last_replay_error'];
        $this->assertSame(RuntimeException::class, $error['class']);
        $this->assertSame('Ledger is down', $error['message']);
    }

    public function testForeignAndUnknownMessagesDoNotSpendTheLimit(): void
    {
        $this->command->unknownTypes = ['user.retired-event'];
        $this->deadLetter('dlq-1', 'source-1', group: 'audit-service');
        $this->deadLetter('dlq-2', 'source-2', 'user.retired-event');
        $this->deadLetter('dlq-3', 'source-3');
        $this->deadLetter('dlq-4', 'source-4');

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --limit=1 --idle-seconds=0'));

        $this->assertCount(1, $this->command->dispatched);
        $this->assertSame('source-3', $this->command->dispatched[0]->context['replayed_from_dlq']['original_message_id']);
        $text = $this->output->fetch();
        $this->assertStringContainsString('limit reached (1)', $text);
        $this->assertMatchesRegularExpression('/Skipped \(other group\)\s*\|?\s*1/', $text);
        $this->assertMatchesRegularExpression('/Skipped \(unknown type\)\s*\|?\s*1/', $text);
    }

    public function testFilteredMessagesDoNotSpendTheLimit(): void
    {
        $this->deadLetter('dlq-1', 'source-1', 'user.deleted');
        $this->deadLetter('dlq-2', 'source-2', 'user.deleted');
        $this->deadLetter('dlq-3', 'source-3');
        $this->deadLetter('dlq-4', 'source-4');

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --type=' . self::TYPE . ' --limit=1 --idle-seconds=0'));

        $this->assertCount(1, $this->command->dispatched);
        $text = $this->output->fetch();
        $this->assertStringContainsString('limit reached (1)', $text);
        $this->assertMatchesRegularExpression('/Skipped \(--type filter\)\s*\|?\s*2/', $text);
    }

    public function testTheRedisOwnerOfAClaimedEntryIsDropped(): void
    {
        $pending = $this->deadLetter('dlq-1', 'source-1');
        $filtered = $this->deadLetter('dlq-2', 'source-2', 'user.deleted');
        $this->stream->queues[self::DLQ] = [];
        $this->stream->pending[self::DLQ] = [$pending, $filtered];

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --type=' . self::TYPE . ' --idle-seconds=0'));

        $this->assertCount(1, $this->command->dispatched);
        $this->assertArrayNotHasKey('owner', $this->command->dispatched[0]->context);
        $parked = $this->stream->publishedTo(self::DLQ);
        $this->assertCount(1, $parked);
        $this->assertArrayNotHasKey('owner', $parked[0]->context);
    }

    public function testAKafkaReplayReadsWithTheEarliestOffsetReset(): void
    {
        $kafka = Mockery::mock(KafkaStream::class);
        $kafka->shouldReceive('setOffsetReset')->once()->with('earliest')->ordered();
        $kafka->shouldReceive('createGroup')->once()->with(self::REPLAY_GROUP, self::DLQ)->ordered()->andReturn(true);
        $kafka->shouldReceive('getIdleMessages')->andReturnUsing(static fn (): Generator => yield from []);
        $kafka->shouldReceive('subscribe')->once()->andReturnUsing(static fn (): Generator => yield from []);
        $kafka->shouldReceive('leaveGroup')->once()->with(self::REPLAY_GROUP, [self::DLQ]);
        $factory = $this->container->get(StreamFactory::class);
        $streams = new ReflectionProperty(StreamFactory::class, 'streams');
        $streams->setValue($factory, [...$streams->getValue($factory), 'kafka' => $kafka]);

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --driver=kafka --idle-seconds=0'));

        $this->assertSame(['leave:' . self::DLQ, 'clear-timers'], $this->command->calls);
    }

    public function testTheGroupOptionReplaysTheDeadLettersOfAnotherGroup(): void
    {
        $dlq = self::SOURCE . '.custom-group.dlq';
        $own = $this->stream->push(new StreamMessage($dlq, self::TYPE, ['userId' => 'u-1'], [
            'dead_letter' => ['source_stream' => self::SOURCE, 'group' => 'custom-group', 'message_id' => 'source-1'],
        ], 'dlq-1'));
        $this->deadLetter('dlq-2', 'source-2');

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --group=custom-group --idle-seconds=0'));

        $this->assertSame(array_fill(0, 2, ['group' => 'custom-group-dlq-replay', 'stream' => $dlq]), $this->stream->subscriptions);
        $this->assertSame([['group' => 'custom-group-dlq-replay', 'stream' => $dlq, 'id' => $own->id]], $this->stream->acked);
        $this->assertCount(1, $this->command->dispatched);
        $this->assertCount(1, $this->stream->queues[self::DLQ]);
    }

    public function testADeadLetterOfTheConfiguredGroupIsForeignUnderAnotherGroup(): void
    {
        $dlq = self::SOURCE . '.custom-group.dlq';
        $this->stream->push(new StreamMessage($dlq, self::TYPE, ['userId' => 'u-1'], [
            'dead_letter' => ['source_stream' => self::SOURCE, 'group' => self::GROUP, 'message_id' => 'source-1'],
        ], 'dlq-1'));

        $this->assertSame(0, $this->replay('--stream=' . self::SOURCE . ' --group=custom-group --idle-seconds=30'));

        $this->assertSame([], $this->command->dispatched);
        $this->assertMatchesRegularExpression('/Skipped \(other group\)\s*\|?\s*1/', $this->output->fetch());
    }

    private function replay(string $arguments): int
    {
        return $this->command->run(new StringInput($arguments . ' --disable-event-dispatcher'), $this->output);
    }

    /**
     * @param  list<StreamMessage>  $messages
     * @return list<?string>
     */
    private function ids(array $messages): array
    {
        return array_map(static fn (StreamMessage $m): ?string => $m->id, $messages);
    }

    private function deadLetter(string $dlqId, string $sourceId, string $type = self::TYPE, int $replays = 0, ?string $group = null): StreamMessage
    {
        $context = [
            'trace_id' => '7b3e0f4c-2a91-4d6e-b8f5-0c1d2e3f4a5b',
            'dead_letter' => [
                'source_stream' => self::SOURCE,
                'group' => $group ?? self::GROUP,
                'message_id' => $sourceId,
                'attempts' => 3,
                'failed_at' => '2026-11-04T10:15:02+00:00',
                'error' => ['class' => 'RuntimeException', 'message' => 'Account not found'],
            ],
        ];

        if ($replays > 0) {
            $context['replayed_from_dlq'] = [
                'at' => '2026-11-05T09:00:00+00:00',
                'original_message_id' => $sourceId,
                'count' => $replays,
            ];
        }

        return $this->stream->push(new StreamMessage(
            stream: self::DLQ,
            type: $type,
            data: [
                'userId' => '0c8a6f2e-5b41-4e3d-9f7a-2d6b1c4e8a90',
                'email' => 'jane@example.com',
            ],
            context: $context,
            id: $dlqId,
        ));
    }
}
