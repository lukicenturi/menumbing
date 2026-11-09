<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Command;

use DateTimeImmutable;
use Generator;
use Hyperf\Command\Command as HyperfCommand;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Coroutine\Coroutine;
use Hyperf\Process\ProcessManager;
use Menumbing\Contract\EventStream\StreamInterface;
use Menumbing\Contract\EventStream\StreamMessage;
use Menumbing\EventStream\DeadLetter\DeadLetterOptions;
use Menumbing\EventStream\Driver\Kafka\KafkaStream;
use Menumbing\EventStream\Driver\Redis\RedisStream;
use Menumbing\EventStream\Enum\ReadMessageFrom;
use Menumbing\EventStream\Enum\Result;
use Menumbing\EventStream\EventRegistry;
use Menumbing\EventStream\Exception\MessageNackedException;
use Menumbing\EventStream\Factory\StreamFactory;
use Menumbing\EventStream\Handler\ConsumerEventHandler;
use Menumbing\EventStream\Listener\RegisterProducers;
use Psr\Container\ContainerInterface;
use RuntimeException;
use stdClass;
use Swoole\Timer;
use Symfony\Component\Console\Input\InputOption;
use Throwable;

use function Hyperf\Support\make;

/**
 * Replays dead letters through the local listeners. Never republished to the source stream: other
 * groups consume it and would run them again. A dry run acks nothing, so it cannot pin the position.
 *
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */
class DlqReplayCommand extends HyperfCommand
{
    public const COMMAND_SIGNATURE = 'event-stream:dlq-replay';

    public const GROUP_SUFFIX = '-dlq-replay';

    private const EMPTY_ROUND_PAUSE = 0.1;

    private const OUTCOME_REPLAYED = 'replayed';

    private const OUTCOME_REPLAY_FAILED = 'replay_failed';

    private const OUTCOME_FILTERED = 'filtered';

    private const OUTCOME_FOREIGN = 'foreign';

    private const OUTCOME_UNKNOWN = 'unknown';

    private const OUTCOME_POISONED = 'poisoned';

    private const OUTCOME_DISCARDED = 'discarded';

    private const OUTCOME_FAILED = 'failed';

    private const SKIPPED_OUTCOMES = [self::OUTCOME_FILTERED, self::OUTCOME_FOREIGN, self::OUTCOME_UNKNOWN];

    protected ?string $name = self::COMMAND_SIGNATURE;

    private static bool $runtimeBooted = false;

    private ?StreamInterface $stream = null;

    private ?ConsumerEventHandler $consumerHandler = null;

    public function __construct(
        protected ContainerInterface $container,
        protected ConfigInterface $config,
    ) {
        parent::__construct();

        $this->setDescription('Replay dead-lettered event stream messages (<stream>.<group>.dlq) into the local listeners.');
    }

    public function configure(): void
    {
        parent::configure();

        $this->setHelp(
            'Messages are dispatched again to the listeners of this application; nothing is published to the source stream. '
            . 'On a dead-letter topic with several partitions, a run with --type or with held messages can stop early; run it again. '
            . 'The group defaults to event_stream.group_name; give --group for a consumer process that sets its own group. '
            . 'CLI only: do not run it from a crontab inside the server.',
        );
        $this->addOption('stream', null, InputOption::VALUE_REQUIRED, 'The source stream, e.g. user-events (not its dead-letter name). Required.');
        $this->addOption('limit', 'l', InputOption::VALUE_REQUIRED, 'Maximum number of messages to replay (or hold) in one run.', '100');
        $this->addOption('type', 't', InputOption::VALUE_REQUIRED, 'Replay only messages with this event name, e.g. user.created.');
        $this->addOption('dry-run', null, InputOption::VALUE_NONE, 'Print only. Nothing is dispatched, re-published or acked.');
        $this->addOption('idle-seconds', null, InputOption::VALUE_REQUIRED, 'Stop when no new message arrives for this many seconds.', '5');
        $this->addOption('max-replays', null, InputOption::VALUE_REQUIRED, 'Hold a message that was already replayed this many times (poison guard).', '3');
        $this->addOption('discard-poisoned', null, InputOption::VALUE_NONE, 'Ack held messages instead of keeping them; the payload goes to a warning log line.');
        $this->addOption('driver', null, InputOption::VALUE_REQUIRED, 'The event stream driver of the source stream.', 'default');
        $this->addOption('group', 'g', InputOption::VALUE_REQUIRED, 'The consumer group whose dead letters to replay (default: event_stream.group_name). For a process that sets its own $groupName.');
    }

    public function handle(): int
    {
        // Inside a server the producers are already registered; a second registration would publish
        // every produced event twice.
        if (ProcessManager::isRunning()) {
            $this->error(sprintf('%s is CLI-only: it must not run inside a server process (e.g. a crontab).', self::COMMAND_SIGNATURE));

            return self::FAILURE;
        }

        $source = trim((string) $this->input->getOption('stream'));
        $options = DeadLetterOptions::fromConfig($this->config);

        if ('' === $source) {
            $this->error('--stream is required: give the name of the source stream.');

            return self::FAILURE;
        }

        if (str_ends_with($source, $options->suffix)) {
            $this->error(sprintf('Give --stream the source stream, not a dead-letter stream (%s).', $source));

            return self::FAILURE;
        }

        $consumerGroup = trim((string) $this->input->getOption('group'));
        $consumerGroup = '' === $consumerGroup ? (string) $this->config->get('event_stream.group_name', 'menumbing') : $consumerGroup;
        $deadLetterStream = $options->streamFor($source, $consumerGroup);
        $group = $consumerGroup . self::GROUP_SUFFIX;
        $limit = max(1, (int) $this->input->getOption('limit'));
        $type = $this->input->getOption('type');
        $type = null === $type || '' === $type ? null : (string) $type;
        $dryRun = (bool) $this->input->getOption('dry-run');
        $idleSeconds = max(0.0, (float) $this->input->getOption('idle-seconds'));
        $run = [
            'group' => $group,
            'consumer_group' => $consumerGroup,
            'dead_letter_stream' => $deadLetterStream,
            'source' => $source,
            'type' => $type,
            'max_replays' => max(0, (int) $this->input->getOption('max-replays')),
            'discard_poisoned' => (bool) $this->input->getOption('discard-poisoned'),
            'dry_run' => $dryRun,
        ];

        if ($dryRun) {
            $this->warn('=== DRY-RUN ===');
            $this->line('Nothing is dispatched, re-published or acked.');
            $this->newLine();
        }

        $this->line(sprintf('Reading %s (group %s) -> local listeners for %s', $deadLetterStream, $group, $source));

        $state = [
            'tally' => array_fill_keys([
                self::OUTCOME_REPLAYED,
                self::OUTCOME_REPLAY_FAILED,
                self::OUTCOME_FILTERED,
                self::OUTCOME_FOREIGN,
                self::OUTCOME_UNKNOWN,
                self::OUTCOME_POISONED,
                self::OUTCOME_DISCARDED,
                self::OUTCOME_FAILED,
            ], 0),
            'handled' => [],
            'counted' => 0,
            'limit' => $limit,
            'stop' => null,
        ];
        $lastActivity = null;
        $readError = null;
        $consumerName = sprintf('%s-replay-%s', $deadLetterStream, gethostname());

        try {
            $this->stream = $this->stream((string) $this->input->getOption('driver'));
            $this->createReplayGroup($group, $deadLetterStream);

            $this->consumeRound($this->pendingMessages($consumerName, $group, $deadLetterStream), $run, $state);

            if ('repeat' === $state['stop']) {
                $state['stop'] = null;
            }

            while (null === $state['stop']) {
                $received = $this->consumeRound($this->stream->subscribe($consumerName, $group, [$deadLetterStream]), $run, $state);

                if ($received || null === $lastActivity) {
                    $lastActivity = microtime(true);
                }

                if (null === $state['stop'] && !$received) {
                    if (microtime(true) - $lastActivity >= $idleSeconds) {
                        $state['stop'] = sprintf('no new message for %s seconds', $idleSeconds);

                        break;
                    }

                    Coroutine::sleep(self::EMPTY_ROUND_PAUSE);
                }
            }
        } catch (Throwable $e) {
            $readError = $e;
        } finally {
            if (null !== $this->stream) {
                $this->leaveGroup($group, $deadLetterStream);
            }

            $this->clearTimers();
        }

        $tally = $state['tally'];
        $stopReason = match ($state['stop']) {
            null => 'read error',
            'repeat' => 'every new message was read',
            'failed' => 're-park to the dead-letter stream failed',
            default => $state['stop'],
        };

        $this->newLine();
        $this->table(['Metric', 'Count'], [
            [$dryRun ? 'Would replay' : 'Replayed', (string) $tally[self::OUTCOME_REPLAYED]],
            ['Failed again (kept on the DLQ)', (string) $tally[self::OUTCOME_REPLAY_FAILED]],
            ['Skipped (--type filter)', (string) $tally[self::OUTCOME_FILTERED]],
            ['Skipped (other group)', (string) $tally[self::OUTCOME_FOREIGN]],
            ['Skipped (unknown type)', (string) $tally[self::OUTCOME_UNKNOWN]],
            ['Held (poison guard)', (string) $tally[self::OUTCOME_POISONED]],
            ['Discarded (--discard-poisoned)', (string) $tally[self::OUTCOME_DISCARDED]],
            ['Re-park failed', (string) $tally[self::OUTCOME_FAILED]],
            ['Dry run', $dryRun ? 'yes' : 'no'],
            ['Stopped because', $stopReason],
        ]);

        if (null !== $readError) {
            $this->error(sprintf('Could not read %s: %s', $deadLetterStream, $readError->getMessage()));
        }

        return null !== $readError || $tally[self::OUTCOME_FAILED] > 0 ? self::FAILURE : self::SUCCESS;
    }

    protected function stream(string $driver): StreamInterface
    {
        return $this->container->get(StreamFactory::class)->get($driver);
    }

    protected function handler(): callable
    {
        return $this->consumerHandler ??= make(ConsumerEventHandler::class, ['eventRegistry' => $this->registry()]);
    }

    protected function knowsType(string $type): bool
    {
        return $this->registry()->has($type);
    }

    protected function bootRuntime(): void
    {
        if (self::$runtimeBooted) {
            return;
        }

        $this->registerRuntime();
        self::$runtimeBooted = true;
    }

    /** Registers the producers a console command lacks; override it to register more at boot. */
    protected function registerRuntime(): void
    {
        $this->container->get(RegisterProducers::class)->process(new stdClass());
    }

    protected function createReplayGroup(string $group, string $deadLetterStream): void
    {
        if ($this->stream instanceof KafkaStream) {
            $this->stream->setOffsetReset('earliest');
        }

        if ($this->stream instanceof RedisStream) {
            $this->stream->createGroup($group, $deadLetterStream, ReadMessageFrom::BEGINNING);

            return;
        }

        $this->stream?->createGroup($group, $deadLetterStream);
    }

    protected function leaveGroup(string $group, string $deadLetterStream): void
    {
        if (!$this->stream instanceof KafkaStream) {
            return;
        }

        try {
            $this->stream->leaveGroup($group, [$deadLetterStream]);
        } catch (Throwable $e) {
            $this->warn(sprintf('Could not leave the consumer group cleanly: %s', $e->getMessage()));
        }
    }

    protected function clearTimers(): void
    {
        if (class_exists(Timer::class)) {
            Timer::clearAll();
        }
    }

    protected function logger(): StdoutLoggerInterface
    {
        return $this->container->get(StdoutLoggerInterface::class);
    }

    protected function registry(): EventRegistry
    {
        $registry = $this->container->get(EventRegistry::class);
        $registry->ensureInitialized();

        return $registry;
    }

    /**
     * @param  iterable<StreamMessage>  $messages
     * @param  array{group: string, consumer_group: string, dead_letter_stream: string, source: string, type: ?string, max_replays: int, discard_poisoned: bool, dry_run: bool}  $run
     * @param  array{tally: array<string, int>, handled: array<string, true>, counted: int, limit: int, stop: ?string}  $state
     *
     * @return bool Whether the round received any message.
     */
    private function consumeRound(iterable $messages, array $run, array &$state): bool
    {
        $received = false;

        foreach ($messages as $message) {
            $received = true;

            $key = $this->lineageKey($message);

            if (isset($state['handled'][$key])) {
                $state['stop'] = 'repeat';

                break;
            }

            $state['handled'][$key] = true;
            $outcome = $this->handleMessage($message, $run);
            ++$state['tally'][$outcome];

            if (self::OUTCOME_FAILED === $outcome) {
                $state['stop'] = 'failed';

                break;
            }

            if (!in_array($outcome, self::SKIPPED_OUTCOMES, true) && ++$state['counted'] >= $state['limit']) {
                $state['stop'] = sprintf('limit reached (%d)', $state['limit']);

                break;
            }
        }

        return $received;
    }

    /**
     * @return Generator<StreamMessage>
     */
    private function pendingMessages(string $consumer, string $group, string $deadLetterStream): Generator
    {
        $stream = $this->stream;

        if (null === $stream) {
            return;
        }

        do {
            $claimed = false;

            foreach ($stream->getIdleMessages($consumer, $group, [$deadLetterStream], 0) as $message) {
                $claimed = true;

                yield $message;
            }
        } while ($claimed);
    }

    /**
     * @param  array{group: string, consumer_group: string, dead_letter_stream: string, source: string, type: ?string, max_replays: int, discard_poisoned: bool, dry_run: bool}  $run
     */
    private function handleMessage(StreamMessage $message, array $run): string
    {
        $deadLetter = (array) ($message->context['dead_letter'] ?? []);
        $previous = (array) ($message->context['replayed_from_dlq'] ?? []);
        $replays = (int) ($previous['count'] ?? 0);
        $originalId = $this->lineageKey($message);
        $summary = sprintf(
            'dlq_id=%s source_id=%s type=%s attempts=%s replays=%d error=%s',
            $message->id,
            $originalId,
            $message->type,
            (string) ($deadLetter['attempts'] ?? '-'),
            $replays,
            (string) ($deadLetter['last_replay_error']['message'] ?? $deadLetter['error']['message'] ?? '-'),
        );

        if (isset($deadLetter['group']) && $deadLetter['group'] !== $run['consumer_group']) {
            return $this->keep($message, $run, self::OUTCOME_FOREIGN);
        }

        if (null !== $run['type'] && $message->type !== $run['type']) {
            return $this->keep($message, $run, self::OUTCOME_FILTERED);
        }

        if (!$this->knowsType($message->type)) {
            $this->warn('UNKNOWN TYPE ' . $summary);

            return $this->keep($message, $run, self::OUTCOME_UNKNOWN);
        }

        if ($replays >= $run['max_replays']) {
            return $this->holdPoisoned($message, $run, $replays, $summary);
        }

        $context = $message->context;
        unset($context['dead_letter'], $context['retry_count'], $context['owner']);
        $context['replayed_from_dlq'] = [
            'at' => (new DateTimeImmutable())->format(DATE_ATOM),
            'original_message_id' => $originalId,
            'count' => $replays + 1,
        ];

        if ($run['dry_run']) {
            $this->line('WOULD REPLAY ' . $summary);

            return self::OUTCOME_REPLAYED;
        }

        $this->bootRuntime();

        $replay = new StreamMessage($run['source'], $message->type, $message->data, [...$context, 'retry_count' => 0], $message->id);

        try {
            ConsumerEventHandler::takeNackReason();
            $result = call_user_func($this->handler(), $run['consumer_group'], $replay);
            $failure = Result::ACK === $result ? null : (ConsumerEventHandler::takeNackReason() ?? MessageNackedException::afterAttempt(1));
        } catch (Throwable $e) {
            $failure = $e;
        }

        if (null === $failure) {
            $this->stream?->ack($run['group'], $run['dead_letter_stream'], [$message->id]);
            $this->info('REPLAYED ' . $summary);

            return self::OUTCOME_REPLAYED;
        }

        $deadLetter['last_replay_error'] = [
            'class' => $failure::class,
            'message' => $failure->getMessage(),
            'at' => $context['replayed_from_dlq']['at'],
        ];
        $retained = new StreamMessage(
            $message->stream,
            $message->type,
            $message->data,
            [...$context, 'dead_letter' => $deadLetter],
            $message->id,
        );

        $this->error(sprintf('FAILED AGAIN %s replay_error=%s', $summary, $failure->getMessage()));

        return $this->park($retained, $run) ? self::OUTCOME_REPLAY_FAILED : self::OUTCOME_FAILED;
    }

    /**
     * @param  array{group: string, dead_letter_stream: string, dry_run: bool}  $run
     */
    private function keep(StreamMessage $message, array $run, string $outcome): string
    {
        if ($run['dry_run']) {
            return $outcome;
        }

        return $this->park($message, $run) ? $outcome : self::OUTCOME_FAILED;
    }

    /**
     * @param  array{group: string, dead_letter_stream: string, discard_poisoned: bool, dry_run: bool}  $run
     */
    private function holdPoisoned(StreamMessage $message, array $run, int $replays, string $summary): string
    {
        $payload = $this->payload($message);

        if (!$run['discard_poisoned']) {
            $this->error(sprintf('HELD (replayed %d times) %s payload=%s', $replays, $summary, $payload));

            return $this->keep($message, $run, self::OUTCOME_POISONED);
        }

        $this->warn(sprintf('DISCARDED (replayed %d times) %s payload=%s', $replays, $summary, $payload));

        if ($run['dry_run']) {
            return self::OUTCOME_DISCARDED;
        }

        $this->logger()->warning(
            'EventStream [Dead-letter discarded] stream={stream} message_id={message_id} type={type} replays={replays} payload={payload}',
            [
                'stream' => $run['dead_letter_stream'],
                'message_id' => (string) $message->id,
                'type' => $message->type,
                'replays' => (string) $replays,
                'payload' => $payload,
            ],
        );
        $this->stream?->ack($run['group'], $run['dead_letter_stream'], [$message->id]);

        return self::OUTCOME_DISCARDED;
    }

    /**
     * Re-publishes, then acks: with the counter ack an unacked message is skipped, not kept. The copy
     * gets a new id (Redis XADD order), so a message without a lineage key gets one first.
     *
     * @param  array{group: string, dead_letter_stream: string}  $run
     */
    private function park(StreamMessage $message, array $run): bool
    {
        $context = $message->context;
        unset($context['retry_count'], $context['owner']);

        if (!isset($context['replayed_from_dlq']['original_message_id']) && !isset($context['dead_letter']['message_id'])) {
            $context['dead_letter'] = [...(array) ($context['dead_letter'] ?? []), 'message_id' => $message->id];
        }

        try {
            $this->stream?->publish(new StreamMessage($run['dead_letter_stream'], $message->type, $message->data, $context));
        } catch (Throwable $e) {
            $this->error(sprintf('COULD NOT RE-PARK dlq_id=%s publish_error=%s', $message->id, $e->getMessage()));

            return false;
        }

        $this->stream?->ack($run['group'], $run['dead_letter_stream'], [$message->id]);

        return true;
    }

    private function lineageKey(StreamMessage $message): string
    {
        return (string) (
            $message->context['replayed_from_dlq']['original_message_id']
            ?? $message->context['dead_letter']['message_id']
            ?? $message->id
        );
    }

    private function payload(StreamMessage $message): string
    {
        return (string) json_encode(
            ['stream' => $message->stream, 'type' => $message->type, 'data' => $message->data, 'context' => $message->context, 'id' => $message->id],
            JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PARTIAL_OUTPUT_ON_ERROR,
        );
    }
}
