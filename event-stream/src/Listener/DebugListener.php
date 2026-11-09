<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Listener;

use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Event\Contract\ListenerInterface;
use Menumbing\EventStream\Event\AfterConsume;
use Menumbing\EventStream\Event\AfterProduce;
use Menumbing\EventStream\Event\BeforeConsume;
use Menumbing\EventStream\Event\BeforeProduce;
use Menumbing\EventStream\Event\ConsumeEvent;
use Menumbing\EventStream\Event\ConsumeFailed;
use Menumbing\EventStream\Event\ConsumerEvent;
use Menumbing\EventStream\Event\ConsumerGroupCreated;
use Menumbing\EventStream\Event\ConsumerGroupCreateFailed;
use Menumbing\EventStream\Event\ConsumerStarted;
use Menumbing\EventStream\Event\ConsumerStopped;
use Menumbing\EventStream\Event\DeadLettered;
use Menumbing\EventStream\Event\ProduceEvent;
use Menumbing\EventStream\Event\ProduceFailed;
use Menumbing\EventStream\Event\SubscribeFailed;

/**
 * @author  Iqbal Maulana <iq.bluejack@gmail.com>
 */
final class DebugListener implements ListenerInterface
{
    public function __construct(
        private StdoutLoggerInterface $logger,
        private ConfigInterface $config,
    ) {
    }

    public function listen(): array
    {
        return [
            AfterConsume::class,
            BeforeConsume::class,
            ConsumeFailed::class,
            ConsumerGroupCreated::class,
            ConsumerGroupCreateFailed::class,
            ConsumerStarted::class,
            ConsumerStopped::class,
            SubscribeFailed::class,
            BeforeProduce::class,
            AfterProduce::class,
            ProduceFailed::class,
            DeadLettered::class,
        ];
    }

    public function process(object $event): void
    {
        // Logged whatever `event_stream.debug` says: this line is the only
        // signal that a message left the live stream and needs a replay.
        if ($event instanceof DeadLettered) {
            $this->logDeadLetteredEvent($event);

            return;
        }

        if (!$this->config->get('event_stream.debug', true)) {
            return;
        }

        match (true) {
            $event instanceof BeforeConsume => $this->logConsumeEvent('Processing', $event),
            $event instanceof AfterConsume => $this->logConsumeEvent('Processed', $event),
            $event instanceof ConsumeFailed => $this->logConsumeFailedEvent($event),
            $event instanceof ConsumerGroupCreated => $this->logConsumerEvent('Consumer group created', $event),
            $event instanceof ConsumerGroupCreateFailed => $this->logConsumerErrorEvent('Consumer group create failed', $event),
            $event instanceof ConsumerStarted => $this->logConsumerEvent('Consumer started', $event),
            $event instanceof ConsumerStopped => $this->logConsumerEvent('Consumer stopped', $event),
            $event instanceof SubscribeFailed => $this->logConsumerErrorEvent('Subscribe failed', $event),
            $event instanceof BeforeProduce => $this->logProduceEvent('Publishing', $event),
            $event instanceof AfterProduce => $this->logProduceEvent('Published', $event),
            $event instanceof ProduceFailed => $this->logProduceFailedEvent($event),
            default => null,
        };
    }

    private function logConsumeEvent(string $status, ConsumeEvent $event): void
    {
        $this->logger->info(
            message: 'EventStream [{status}] consumer={consumer} group={group} stream={stream} message_id={message_id} type={type} attempt={attempt} driver={driver}',
            context: [
                'status' => $status,
                'consumer' => $event->consumerName,
                'group' => $event->groupName,
                'stream' => $event->message->stream,
                'message_id' => $event->message->id,
                'type' => $event->message->type,
                'attempt' => $event->message->context['retry_count'] ?? 0,
                'driver' => $event->streamDriver,
            ],
        );
    }

    private function logConsumeFailedEvent(ConsumeFailed $event): void
    {
        $this->logger->error(
            message: 'EventStream [Consume failed] consumer={consumer} group={group} stream={stream} message_id={message_id} type={type} attempt={attempt} driver={driver} error={error}',
            context: [
                'consumer' => $event->consumerName,
                'group' => $event->groupName,
                'stream' => $event->message->stream,
                'message_id' => $event->message->id,
                'type' => $event->message->type,
                'attempt' => $event->message->context['retry_count'] ?? 0,
                'driver' => $event->streamDriver,
                'error' => $event->exception->getMessage(),
                'exception' => (string) $event->exception,
            ],
        );
    }

    private function logDeadLetteredEvent(DeadLettered $event): void
    {
        $this->logger->error(
            message: 'EventStream [Dead-lettered] consumer={consumer} group={group} stream={stream} dead_letter_stream={dead_letter_stream} message_id={message_id} type={type} attempts={attempts} error={error}',
            context: [
                'consumer' => $event->consumerName,
                'group' => $event->groupName,
                'stream' => $event->streamName,
                'dead_letter_stream' => $event->deadLetterStream,
                'message_id' => (string) $event->message->id,
                'type' => $event->message->type,
                'attempts' => $event->attempts,
                'error' => $event->throwable->getMessage(),
                'error_class' => $event->throwable::class,
                'exception' => (string) $event->throwable,
            ],
        );
    }

    private function logConsumerEvent(string $status, ConsumerEvent $event): void
    {
        $this->logger->info(
            message: 'EventStream [{status}] consumer={consumer} group={group} stream={stream} driver={driver}',
            context: [
                'status' => $status,
                'consumer' => $event->consumerName,
                'group' => $event->groupName,
                'stream' => $event->streamName,
                'driver' => $event->streamDriver,
            ],
        );
    }

    private function logConsumerErrorEvent(string $status, ConsumerEvent $event): void
    {
        $exception = match (true) {
            $event instanceof ConsumerGroupCreateFailed => $event->exception,
            $event instanceof SubscribeFailed => $event->exception,
            default => null,
        };

        $context = [
            'status' => $status,
            'consumer' => $event->consumerName,
            'group' => $event->groupName,
            'stream' => $event->streamName,
            'driver' => $event->streamDriver,
        ];

        if (null !== $exception) {
            $context['error'] = $exception->getMessage();
            $context['exception'] = (string) $exception;
        }

        $this->logger->error(
            message: 'EventStream [{status}] consumer={consumer} group={group} stream={stream} driver={driver}' . (null !== $exception ? ' error={error}' : ''),
            context: $context,
        );
    }

    private function logProduceEvent(string $status, ProduceEvent $event): void
    {
        $context = [
            'status' => $status,
            'stream' => $event->message->stream,
            'message_id' => $event->message->id,
            'type' => $event->message->type,
            'driver' => $event->streamDriver,
        ];

        if (null !== $event->endTime) {
            $context['duration_ms'] = round(($event->endTime - $event->startTime) * 1000, 2);
        }

        $this->logger->info(
            message: 'EventStream [{status}] stream={stream} message_id={message_id} type={type} driver={driver}' . (isset($context['duration_ms']) ? ' duration={duration_ms}ms' : ''),
            context: $context,
        );
    }

    private function logProduceFailedEvent(ProduceFailed $event): void
    {
        $context = [
            'stream' => $event->message->stream,
            'message_id' => $event->message->id,
            'type' => $event->message->type,
            'driver' => $event->streamDriver,
            'error' => $event->throwable->getMessage(),
            'exception' => (string) $event->throwable,
        ];

        if (null !== $event->endTime) {
            $context['duration_ms'] = round(($event->endTime - $event->startTime) * 1000, 2);
        }

        $this->logger->error(
            message: 'EventStream [Produce failed] stream={stream} message_id={message_id} type={type} driver={driver} error={error}',
            context: $context,
        );
    }
}
