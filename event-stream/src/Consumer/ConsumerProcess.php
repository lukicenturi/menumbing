<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Consumer;

use DateTimeImmutable;
use Generator;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Coordinator\Constants;
use Hyperf\Coordinator\CoordinatorManager;
use Hyperf\Coroutine\Coroutine;
use Hyperf\Engine\Channel;
use Hyperf\Process\AbstractProcess;
use Hyperf\Process\ProcessManager;
use Menumbing\Contract\EventStream\StreamInterface;
use Menumbing\Contract\EventStream\StreamMessage;
use Menumbing\EventStream\DeadLetter\DeadLetterOptions;
use Menumbing\EventStream\Enum\Result;
use Menumbing\EventStream\Event\AfterConsume;
use Menumbing\EventStream\Event\BeforeConsume;
use Menumbing\EventStream\Event\ConsumeFailed;
use Menumbing\EventStream\Event\ConsumerGroupCreated;
use Menumbing\EventStream\Event\ConsumerGroupCreateFailed;
use Menumbing\EventStream\Event\ConsumerStarted;
use Menumbing\EventStream\Event\ConsumerStopped;
use Menumbing\EventStream\Event\DeadLettered;
use Menumbing\EventStream\Event\SubscribeFailed;
use Menumbing\EventStream\EventRegistry;
use Menumbing\EventStream\Exception\MessageNackedException;
use Menumbing\EventStream\Factory\StreamFactory;
use Menumbing\EventStream\Handler\ConsumerEventHandler;
use Psr\Container\ContainerInterface;
use Throwable;

/**
 * @author  Iqbal Maulana <iq.bluejack@gmail.com>
 */
abstract class ConsumerProcess extends AbstractProcess
{
    public ?string $streamName = null;

    public string $driverName = 'default';

    public ?string $groupName = null;

    public int $concurrent = 1;

    protected int $restartInterval = 1;

    protected StreamInterface $stream;

    protected EventRegistry $eventRegistry;

    protected ConfigInterface $config;

    protected ?string $consumerName = null;

    protected array $retryCounters = [];

    public function __construct(ContainerInterface $container)
    {
        parent::__construct($container);

        $this->stream = $container->get(StreamFactory::class)->get($this->driverName);
        $this->eventRegistry = $container->get(EventRegistry::class);
        $this->config = $container->get(ConfigInterface::class);
    }

    public function handle(): void
    {
        $consumerName = $this->getConsumerName();

        try {
            $this->stream->createGroup($this->groupName, $this->streamName);
            $this->event?->dispatch(new ConsumerGroupCreated($consumerName, $this->groupName, $this->streamName, $this->driverName));
        } catch (Throwable $e) {
            $this->event?->dispatch(new ConsumerGroupCreateFailed($consumerName, $this->groupName, $this->streamName, $this->driverName, $e));
            $this->logThrowable($e);

            return;
        }

        $this->event?->dispatch(new ConsumerStarted($consumerName, $this->groupName, $this->streamName, $this->driverName));

        // Set instance ID for Kafka static membership (deterministic group_instance_id).
        // This must happen before subscribe() is called so the consumer factory can use it.
        if (method_exists($this->stream, 'setConsumerName')) {
            $this->stream->setConsumerName($this->getInstanceId());
        }

        Coroutine::create(function () use ($consumerName) {
            $retryAfter = $this->config->get('event_stream.consumer.retry_after', 60) * 1000;

            while (ProcessManager::isRunning()) {
                $messages = $this->stream->getIdleMessages($consumerName, $this->groupName, [$this->streamName], $retryAfter);

                // Same guard as the subscribe loop below: a throw out of
                // processMessage() (an ack failure, or a DeadLettered listener
                // that threw after the ack) must not end this coroutine, or
                // pending entries would never be reclaimed again until restart.
                try {
                    foreach ($messages as $message) {
                        $this->processMessage($message);
                    }
                } catch (Throwable $e) {
                    $this->logThrowable($e);
                }

                if (CoordinatorManager::until(Constants::WORKER_EXIT)->yield($this->config->get('event_stream.consumer.block_for', 1))) {
                    break;
                }
            }
        });

        while (ProcessManager::isRunning()) {
            try {
                $messages = $this->stream->subscribe($consumerName, $this->groupName, [$this->streamName]);

                if ($this->concurrent <= 1) {
                    foreach ($messages as $message)  {
                        $this->processMessage($message);
                    }
                } else {
                    $this->processBatch($messages);
                }
            } catch (Throwable $e) {
                if (!$this->shouldSuppressRecoverableSubscribeError($e)) {
                    $this->event?->dispatch(new SubscribeFailed($consumerName, $this->groupName, $this->streamName, $this->driverName, $e));
                    $this->logThrowable($e);
                }
            }

            if (CoordinatorManager::until(Constants::WORKER_EXIT)->yield($this->config->get('event_stream.consumer.block_for', 1))) {
                $this->event?->dispatch(new ConsumerStopped($consumerName, $this->groupName, $this->streamName, $this->driverName));
                break;
            }
        }
    }

    /**
     * Handles one message: retried in place, then dead-lettered.
     *
     * A failed message is never handed back by the Kafka driver: its
     * `getIdleMessages()` yields nothing, and the next ack of any later
     * message commits the offset past the failed one. Without the loop below,
     * one exception in a listener was a silent, permanent loss on Kafka.
     *
     * So a message is retried here, with an exponential pause, up to
     * `dead_letter.max_attempts` times. A message that still fails is copied
     * to `<stream>.<group><suffix>` with a `dead_letter` block in its context,
     * and only then acked, so it can be replayed with `event-stream:dlq-replay`.
     *
     * Every message is acked exactly once, whatever happens to it. With the
     * longlang/phpkafka client an ack is not a per-message mark: `ack()` is
     * `OffsetManager::addFetchOffset(partition, +1)` plus a commit, a counter
     * per partition, whichever message it names. A message that is never acked
     * is not redelivered; the counter only lags by one, so the failed message
     * is skipped all the same and the last message of the batch is fetched
     * again as a duplicate.
     *
     * With `concurrent` > 1, processBatch() runs this method once per message
     * in its own coroutine, so all retry state stays local to this call and
     * the acks land in completion order. With the counter-style ack only the
     * number of acks matters, and it is right once the whole batch is done.
     *
     * `#[ConsumedEvent(retries: n)]` shares the same budget: the attempt index
     * goes into `retry_count`, so ConsumerEventHandler answers NACK while
     * `retry_count < n` and rethrows after. Either answer counts as one failed
     * attempt, and `max_attempts` is the ceiling for both. For a NACK, the
     * exception that the handler caught is used as the failure, so the real
     * error reaches `ConsumeFailed` and `dead_letter.error` also when
     * `n >= max_attempts` and no attempt rethrows.
     */
    protected function processMessage(StreamMessage $message): void
    {
        $options = $this->deadLetterOptions();

        if (!$options->enabled) {
            $this->processMessageWithoutDeadLetter($message);

            return;
        }

        $consumerName = $this->getConsumerName();
        $attempts = 0;
        $failure = null;

        while ($attempts < $options->maxAttempts) {
            $attempt = $message->withContext(['retry_count' => $attempts]);
            ++$attempts;

            $this->event?->dispatch(new BeforeConsume($consumerName, $this->groupName, $attempt, $this->stream, $this->driverName));

            ConsumerEventHandler::takeNackReason();

            try {
                $result = call_user_func($this->handler(), $this->groupName, $attempt);
            } catch (Throwable $e) {
                $result = $e;
            }

            if (Result::ACK === $result) {
                $this->stream->ack($this->groupName, $this->streamName, [$message->id]);
                $this->event?->dispatch(new AfterConsume($consumerName, $this->groupName, $attempt, $this->stream, $this->driverName));

                return;
            }

            $failure = $result instanceof Throwable
                ? $result
                : (ConsumerEventHandler::takeNackReason() ?? MessageNackedException::afterAttempt($attempts));

            // One `EventStream [Consume failed]` line per failed attempt.
            $this->event?->dispatch(new ConsumeFailed($consumerName, $this->groupName, $attempt, $this->stream, $this->driverName, $failure));

            // A skip-listed exception is deterministic (bad payload, broken
            // invariant): another attempt would fail the same way.
            if ($result instanceof Throwable && $this->skipException($result)) {
                break;
            }

            if ($attempts < $options->maxAttempts) {
                $this->pause($options->backoffAfter($attempts));
            }
        }

        $this->deadLetter($message, $attempts, $failure, $options);
    }

    /**
     * The behaviour before the dead-letter path, kept for
     * `dead_letter.enabled = false`: one attempt per delivery, a NACK leaves
     * the message unacked for the driver to hand back, and a skip-listed
     * exception is acked and dropped.
     */
    protected function processMessageWithoutDeadLetter(StreamMessage $message): void
    {
        $consumerName = $this->getConsumerName();

        $retryCount = $this->retryCounters[$message->id] ?? 0;
        $message = $message->withContext([...$message->context, 'retry_count' => $retryCount]);

        $this->event?->dispatch(new BeforeConsume($consumerName, $this->groupName, $message, $this->stream, $this->driverName));

        try {
            $result = call_user_func($this->handler(), $this->groupName, $message);
            // Not used on this path; dropped so the context does not hold it.
            ConsumerEventHandler::takeNackReason();

            if (Result::ACK === $result) {
                unset($this->retryCounters[$message->id]);
                $this->stream->ack($this->groupName, $this->streamName, [$message->id]);

                $this->event?->dispatch(new AfterConsume($consumerName, $this->groupName, $message, $this->stream, $this->driverName));
            } elseif (Result::NACK === $result) {
                $this->retryCounters[$message->id] = $retryCount + 1;
            }
        } catch (Throwable $e) {
            if ($this->skipException($e)) {
                unset($this->retryCounters[$message->id]);
                $this->stream->ack($this->groupName, $this->streamName, [$message->id]);
            }

            $this->event?->dispatch(new ConsumeFailed($consumerName, $this->groupName, $message, $this->stream, $this->driverName, $e));
        }
    }

    /**
     * Copies the message to the dead-letter stream, then acks the original.
     */
    protected function deadLetter(StreamMessage $message, int $attempts, Throwable $failure, DeadLetterOptions $options): void
    {
        $deadLetterStream = $options->streamFor((string) $this->streamName, (string) $this->groupName);
        $context = $message->context;
        // `owner` is added by RedisStream::getIdleMessages() to a claimed
        // entry; like `retry_count`, it describes this delivery, not the message.
        unset($context['retry_count'], $context['owner']);

        // No id: the driver gives the copy a new one. Redis only accepts a new
        // entry id above the last one of the target stream, so the source id
        // cannot be reused; the source id is kept in `dead_letter.message_id`.
        $copy = new StreamMessage(
            stream: $deadLetterStream,
            type: $message->type,
            data: $message->data,
            context: [
                ...$context,
                'dead_letter' => [
                    'source_stream' => $this->streamName,
                    'group' => $this->groupName,
                    'message_id' => $message->id,
                    'attempts' => $attempts,
                    'failed_at' => (new DateTimeImmutable())->format(DATE_ATOM),
                    'error' => [
                        'class' => $failure::class,
                        'message' => $failure->getMessage(),
                    ],
                ],
            ],
        );

        try {
            $this->stream->publish($copy);
        } catch (Throwable $e) {
            // Acked anyway. Leaving it unacked would not keep it on Kafka (see
            // processMessage()): it would still be skipped, and a good message
            // would be redelivered in its place. So this log line is the last
            // copy: it carries the full payload, ready to publish to the
            // dead-letter stream by hand. It is written by the process itself,
            // not by a listener, so `event_stream.debug` cannot turn it off,
            // and it is written before the ack, so it exists even if the ack
            // throws.
            $this->logger()->critical(
                'EventStream [Dead-letter publish failed] consumer={consumer} group={group} stream={stream} dead_letter_stream={dead_letter_stream} message_id={message_id} type={type} attempts={attempts} error={error} publish_error={publish_error} payload={payload}',
                [
                    'consumer' => $this->getConsumerName(),
                    'group' => (string) $this->groupName,
                    'stream' => (string) $this->streamName,
                    'dead_letter_stream' => $deadLetterStream,
                    'message_id' => (string) $message->id,
                    'type' => $message->type,
                    'attempts' => (string) $attempts,
                    'error' => $failure->getMessage(),
                    'publish_error' => $e->getMessage(),
                    'payload' => (string) json_encode(
                        ['stream' => $copy->stream, 'type' => $copy->type, 'data' => $copy->data, 'context' => $copy->context],
                        JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PARTIAL_OUTPUT_ON_ERROR,
                    ),
                ],
            );
            $this->stream->ack($this->groupName, $this->streamName, [$message->id]);

            return;
        }

        $listenerFailure = null;

        try {
            $this->event?->dispatch(new DeadLettered(
                $this->getConsumerName(),
                (string) $this->groupName,
                (string) $this->streamName,
                $deadLetterStream,
                $message,
                $attempts,
                $failure,
            ));
        } catch (Throwable $e) {
            // A listener of DeadLettered that throws must not leave the
            // original unacked (see processMessage()). It is logged before the
            // ack, so the record of where the message went survives an ack
            // that throws too; then it is rethrown.
            $listenerFailure = $e;
            $this->logger()->error(
                'EventStream [Dead-lettered listener failed] consumer={consumer} group={group} stream={stream} dead_letter_stream={dead_letter_stream} message_id={message_id} type={type} error={error}',
                [
                    'consumer' => $this->getConsumerName(),
                    'group' => (string) $this->groupName,
                    'stream' => (string) $this->streamName,
                    'dead_letter_stream' => $deadLetterStream,
                    'message_id' => (string) $message->id,
                    'type' => $message->type,
                    'error' => $e->getMessage(),
                    'exception' => (string) $e,
                ],
            );
        }

        $this->stream->ack($this->groupName, $this->streamName, [$message->id]);

        if (null !== $listenerFailure) {
            throw $listenerFailure;
        }
    }

    protected function deadLetterOptions(): DeadLetterOptions
    {
        return DeadLetterOptions::fromConfig($this->config);
    }

    /**
     * Coroutine-aware under the sleep hook (SWOOLE_HOOK_SLEEP), so a pause
     * blocks this message only. Kafka heartbeats run on a timer and keep going
     * meanwhile.
     */
    protected function pause(int $milliseconds): void
    {
        if ($milliseconds > 0) {
            Coroutine::sleep($milliseconds / 1000);
        }
    }

    protected function logger(): StdoutLoggerInterface
    {
        return $this->container->get(StdoutLoggerInterface::class);
    }

    protected function processBatch(Generator $messages): void
    {
        $batch = [];
        foreach ($messages as $message) {
            $batch[] = $message;
        }

        if (empty($batch)) {
            return;
        }

        $channel = new Channel(count($batch));

        foreach ($batch as $message) {
            Coroutine::create(function () use ($message, $channel) {
                try {
                    $this->processMessage($message);
                } finally {
                    $channel->push(true);
                }
            });
        }

        for ($i = 0; $i < count($batch); $i++) {
            $channel->pop();
        }
    }

    protected function getConsumerName(): string
    {
        return $this->consumerName ??= sprintf('%s-%s', $this->streamName, gethostname());
    }

    /**
     * Derive a deterministic instance ID for Kafka static membership.
     * Combines process name (which includes process index when nums > 1) with hostname,
     * making it unique per pod+process while remaining stable across pod restarts.
     */
    protected function getInstanceId(): string
    {
        // $this->name = e.g. "default:loan-events" or "default:loan-repayment-events.1"
        // Convert to kebab-case: replace colons and underscores with hyphens
        $processName = str_replace([':', '_'], '-', $this->name);

        return $processName . '-' . gethostname();
    }

    protected function shouldSuppressRecoverableSubscribeError(Throwable $e): bool
    {
        if (!$this->config->get('event_stream.consumer.suppress_recoverable_subscribe_errors', false)) {
            return false;
        }

        if (!is_a($e, 'longlang\\phpkafka\\Exception\\KafkaErrorException')) {
            return false;
        }

        return in_array($e->getCode(), $this->config->get('event_stream.consumer.recoverable_subscribe_error_codes', [
            14, // COORDINATOR_LOAD_IN_PROGRESS
            15, // COORDINATOR_NOT_AVAILABLE
            16, // NOT_COORDINATOR
            25, // UNKNOWN_MEMBER_ID
            27, // REBALANCE_IN_PROGRESS
            82, // FENCED_INSTANCE_ID
        ]), true);
    }

    protected function skipException(Throwable $e): bool
    {
        $skipExceptions = $this->config->get('event_stream.skip_exceptions', []);

        if (empty($skipExceptions)) {
            return false;
        }

        foreach ($skipExceptions as $skipException) {
            if (is_a($e, $skipException, true)
                || is_subclass_of($e, $skipException)
                || in_array($skipException, class_implements($e), true)) {
                return true;
            }
        }

        return false;
    }

    abstract protected function handler(): callable;
}
