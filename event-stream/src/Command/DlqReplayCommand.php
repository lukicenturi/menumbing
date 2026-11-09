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
 * Replays dead-lettered messages from `<stream>.<group><suffix>` into the
 * listeners of this application.
 *
 * The replay is LOCAL: each message goes through ConsumerEventHandler in this
 * process, exactly as the consumer process would have dispatched it. Nothing
 * is published to `<stream>`. A source stream is often a fan-out stream, so a
 * copy published there would be consumed again by every other group on it,
 * and their listeners would do the same work twice.
 *
 * Reads with its own consumer group, `<group_name>-dlq-replay`. On Kafka a
 * group with no committed offset starts at offset 0 with longlang/phpkafka, so
 * the first run reads the whole retained dead-letter stream and later runs
 * resume where the last one stopped. On Redis the group is created at the
 * start of the stream for the same reason.
 *
 * Three traits of the Kafka client shape this command:
 *
 * - An ack is a counter per partition, not a mark per message: it moves the
 *   committed offset one step forward, whichever message it names. A message
 *   left unacked between two acked ones is therefore skipped anyway. So a
 *   message that must stay on the dead-letter stream (filtered out by --type,
 *   failed again, held by the poison guard, or dead-lettered by another group)
 *   is re-parked: published to the dead-letter stream again and then acked. It
 *   moves to the end of the stream instead of falling out of it.
 *
 * - Nothing advances the fetch position but an ack. A dry run acks nothing, so
 *   every fetch returns the same messages again. The run stops at the first
 *   message it has already handled. In a dry run that is the start of the same
 *   batch again; in a real run it is the first message this run re-parked.
 *   Either way, everything new has been seen.
 *
 * - One fetch reads from one partition leader. On a dead-letter topic with
 *   several partitions, a run that re-parks messages can reach its own
 *   re-parked copy on one partition before it has read the others, and stop
 *   early. Run it again; the next run starts where this one stopped.
 *
 * Each subscribe() call waits a fraction of a second for a fetch and then
 * returns, so --idle-seconds is a plain clock check between calls.
 *
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */
class DlqReplayCommand extends HyperfCommand
{
    public const COMMAND_SIGNATURE = 'event-stream:dlq-replay';

    public const GROUP_SUFFIX = '-dlq-replay';

    /** Pause between two empty fetches, so a fast driver does not spin. */
    private const EMPTY_ROUND_PAUSE = 0.1;

    private const OUTCOME_REPLAYED = 'replayed';

    private const OUTCOME_REPLAY_FAILED = 'replay_failed';

    private const OUTCOME_FILTERED = 'filtered';

    private const OUTCOME_FOREIGN = 'foreign';

    private const OUTCOME_UNKNOWN = 'unknown';

    private const OUTCOME_POISONED = 'poisoned';

    private const OUTCOME_DISCARDED = 'discarded';

    private const OUTCOME_FAILED = 'failed';

    /** Outcomes that keep a message without spending the --limit. */
    private const SKIPPED_OUTCOMES = [self::OUTCOME_FILTERED, self::OUTCOME_FOREIGN, self::OUTCOME_UNKNOWN];

    protected ?string $name = self::COMMAND_SIGNATURE;

    /**
     * Producers are registered once per process: registering them twice would
     * publish every produced event twice.
     */
    private static bool $runtimeBooted = false;

    private ?StreamInterface $stream = null;

    private ?ConsumerEventHandler $consumerHandler = null;

    public function __construct(
        protected ContainerInterface $container,
        protected ConfigInterface $config,
    ) {
        // The name comes from `$name`, so a subclass can give itself another one.
        parent::__construct();

        $this->setDescription('Replay dead-lettered event stream messages (<stream>.<group>.dlq) into the local listeners.');
    }

    public function configure(): void
    {
        parent::configure();

        $this->setHelp(
            'Messages are dispatched again to the listeners of this application; nothing is published to the source stream. '
            . 'On a dead-letter topic with several partitions, a run with --type or with held messages can stop early; run it again. '
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
    }

    public function handle(): int
    {
        // Inside a server worker the producers are already registered, and
        // the second registration this command needs would publish every
        // produced event twice for the life of the worker.
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

        $consumerGroup = (string) $this->config->get('event_stream.group_name', 'menumbing');
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

            // Entries an earlier run left pending in the replay group (a dry
            // run, or a crash before an ack). Only Redis keeps those; the
            // Kafka driver yields nothing here. A repeat ends this phase only:
            // in a dry run every claimed entry stays pending and comes back.
            $this->consumeRound($this->pendingMessages($consumerName, $group, $deadLetterStream), $run, $state);

            if ('repeat' === $state['stop']) {
                $state['stop'] = null;
            }

            while (null === $state['stop']) {
                $received = $this->consumeRound($this->stream->subscribe($consumerName, $group, [$deadLetterStream]), $run, $state);

                // The first call also joins the consumer group, which can take
                // a while; the idle clock starts once that is done.
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
            // Also after a failed first fetch: it may have joined the group
            // and started the heartbeat timer already, and that timer alone
            // keeps the CLI process alive after handle() returns.
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

    /**
     * ConsumerEventHandler, as the consumer process uses it: it maps the type
     * to the consumed event class and dispatches it to the local listeners.
     */
    protected function handler(): callable
    {
        return $this->consumerHandler ??= make(ConsumerEventHandler::class, ['eventRegistry' => $this->registry()]);
    }

    /**
     * ConsumerEventHandler answers ACK, without dispatching anything, for a
     * type it does not know. That is right on a fan-out source stream and
     * wrong here: a dead letter whose event class no longer exists would be
     * acked and gone.
     */
    protected function knowsType(string $type): bool
    {
        return $this->registry()->has($type);
    }

    /**
     * Runs registerRuntime() once per process, before the first local dispatch.
     */
    protected function bootRuntime(): void
    {
        if (self::$runtimeBooted) {
            return;
        }

        $this->registerRuntime();
        self::$runtimeBooted = true;
    }

    /**
     * A console command never fires BeforeMainServerStart, which is what
     * registers the producers. Without them, a replayed listener that
     * publishes an event would publish nothing. RegisterProducers ignores the
     * event it is given. handle() already refused to run inside a server,
     * where this registration would be the second one.
     *
     * An application whose listeners need more boot-time registration (for
     * example HTTP clients registered on server start) extends this command,
     * overrides this method and calls the parent.
     */
    protected function registerRuntime(): void
    {
        $this->container->get(RegisterProducers::class)->process(new stdClass());
    }

    /**
     * Kafka has nothing to create. On Redis the replay group must start at the
     * beginning of the stream: the dead letters were written before it existed.
     */
    protected function createReplayGroup(string $group, string $deadLetterStream): void
    {
        if ($this->stream instanceof RedisStream) {
            $this->stream->createGroup($group, $deadLetterStream, ReadMessageFrom::BEGINNING);

            return;
        }

        $this->stream?->createGroup($group, $deadLetterStream);
    }

    /**
     * Leaves the Kafka group and stops its heartbeat, so the static member
     * does not hold its partitions for the session timeout. Only a consumer
     * that subscribe() already built is closed.
     */
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

    /**
     * The last guard against a hung exit: a heartbeat timer that close() did
     * not stop keeps the event loop alive. The command is about to exit, so no
     * timer is worth keeping.
     */
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

    /**
     * The shared registry. In a server, ConsumerManager fills it at boot; a
     * console command never gets there, so it is filled here, once.
     */
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

            // Keyed on the lineage, not on the message id: every copy of one
            // failed message shares this key, and a re-parked copy gets a new
            // id. A crash between a re-park and its ack also leaves two copies.
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

        // Another group's failure on a shared stream (a group with another
        // name, or a stream written by hand): not ours to run.
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
        unset($context['dead_letter'], $context['retry_count']);
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

        // retry_count 0 and one attempt: a NACK (an event with retries left)
        // counts as a failure like a throw does, and the message goes back to
        // the dead-letter stream for the next run.
        $replay = new StreamMessage($run['source'], $message->type, $message->data, [...$context, 'retry_count' => 0], $message->id);

        try {
            $result = call_user_func($this->handler(), $run['consumer_group'], $replay);
            $failure = Result::ACK === $result ? null : MessageNackedException::afterAttempt(1);
        } catch (Throwable $e) {
            $failure = $e;
        }

        if (null === $failure) {
            $this->stream?->ack($run['group'], $run['dead_letter_stream'], [$message->id]);
            $this->info('REPLAYED ' . $summary);

            return self::OUTCOME_REPLAYED;
        }

        // Back to the dead-letter stream with this replay counted, so the
        // poison guard stops a message that can never succeed. The first
        // failure stays on record in `dead_letter.error`.
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

        // The log line is the only copy left once this ack lands.
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
     * Keeps a message on the dead-letter stream: same lineage, moved to the
     * end, then acked. If the publish fails nothing is acked and the run stops,
     * so the committed position stays on this message and the next run starts
     * with it.
     *
     * The copy gets a new id from the driver: Redis only accepts a new entry
     * id above the last one of the stream. The lineage key is what stays, so a
     * message without one gets its current id written into
     * `dead_letter.message_id` first; else its copy would look new to this
     * run and be read again and again.
     *
     * @param  array{group: string, dead_letter_stream: string}  $run
     */
    private function park(StreamMessage $message, array $run): bool
    {
        $context = $message->context;
        unset($context['retry_count']);

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

    /**
     * The id of the first failed message; it stays the same across repeated
     * dead-letter and replay rounds.
     */
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
