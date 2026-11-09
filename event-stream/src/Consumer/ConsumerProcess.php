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
     * Retries in place, then dead-letters. Every message is acked exactly once: the longlang ack is a
     * per-partition offset counter, so an unacked message is skipped anyway and the last one re-fetched.
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

            $this->dispatchGuarded(new BeforeConsume($consumerName, $this->groupName, $attempt, $this->stream, $this->driverName), $attempt);

            ConsumerEventHandler::takeNackReason();

            try {
                $result = call_user_func($this->handler(), $this->groupName, $attempt);
            } catch (Throwable $e) {
                $result = $e;
            }

            if (Result::ACK === $result) {
                $this->stream->ack($this->groupName, $this->streamName, [$message->id]);
                $this->dispatchGuarded(new AfterConsume($consumerName, $this->groupName, $attempt, $this->stream, $this->driverName), $attempt);

                return;
            }

            $failure = $result instanceof Throwable
                ? $result
                : (ConsumerEventHandler::takeNackReason() ?? MessageNackedException::afterAttempt($attempts));

            $this->dispatchGuarded(new ConsumeFailed($consumerName, $this->groupName, $attempt, $this->stream, $this->driverName, $failure), $attempt);

            // `$failure`, not `$result`: with `retries: n` the handler wraps a skip-listed exception in a NACK.
            if (!$failure instanceof MessageNackedException && $this->skipException($failure)) {
                break;
            }

            if ($attempts < $options->maxAttempts) {
                $this->pause($options->backoffAfter($attempts));
            }
        }

        $this->deadLetter($message, $attempts, $failure, $options);
    }

    /** The behaviour before the dead-letter path, for `dead_letter.enabled = false`. */
    protected function processMessageWithoutDeadLetter(StreamMessage $message): void
    {
        $consumerName = $this->getConsumerName();

        $retryCount = $this->retryCounters[$message->id] ?? 0;
        $message = $message->withContext([...$message->context, 'retry_count' => $retryCount]);

        $this->event?->dispatch(new BeforeConsume($consumerName, $this->groupName, $message, $this->stream, $this->driverName));

        try {
            $result = call_user_func($this->handler(), $this->groupName, $message);
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

    protected function deadLetter(StreamMessage $message, int $attempts, Throwable $failure, DeadLetterOptions $options): void
    {
        $deadLetterStream = $options->streamFor((string) $this->streamName, (string) $this->groupName);
        $context = $message->context;
        unset($context['retry_count'], $context['owner']);

        // No id: Redis XADD rejects an id below the stream's last; the source id is in `dead_letter.message_id`.
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
            // Acked anyway: with the counter ack an unacked message is lost all the same. This line is the
            // last copy, so it is logged here before the ack, not by a listener that `debug` can turn off.
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

        $this->dispatchGuarded(
            new DeadLettered(
                $this->getConsumerName(),
                (string) $this->groupName,
                (string) $this->streamName,
                $deadLetterStream,
                $message,
                $attempts,
                $failure,
            ),
            $message,
            ['dead_letter_stream' => $deadLetterStream],
        );

        $this->stream->ack($this->groupName, $this->streamName, [$message->id]);
    }

    /**
     * Logs a throwing listener, so it cannot skip the ack or the dead letter.
     *
     * @param  array<string, string>  $extra
     */
    protected function dispatchGuarded(object $event, StreamMessage $message, array $extra = []): void
    {
        try {
            $this->event?->dispatch($event);
        } catch (Throwable $e) {
            $label = $event instanceof DeadLettered ? 'Dead-lettered listener failed' : 'Listener failed';
            $fields = [
                'consumer' => $this->getConsumerName(),
                'group' => (string) $this->groupName,
                'stream' => (string) $this->streamName,
                ...$extra,
                'message_id' => (string) $message->id,
                'type' => $message->type,
            ];
            $placeholders = implode(' ', array_map(static fn (string $key): string => sprintf('%s={%s}', $key, $key), array_keys($fields)));

            $this->logger()->error(
                sprintf('EventStream [%s] event={event} %s error={error}', $label, $placeholders),
                [
                    ...$fields,
                    'event' => $event::class,
                    'error' => $e->getMessage(),
                    'exception' => (string) $e,
                ],
            );
        }
    }

    protected function deadLetterOptions(): DeadLetterOptions
    {
        return DeadLetterOptions::fromConfig($this->config);
    }

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
