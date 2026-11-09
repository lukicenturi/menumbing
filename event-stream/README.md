# Event Stream for Hyperf

A powerful event streaming component for Hyperf applications, providing a simple way to produce and consume events using stream engine like Redis and Kafka.

## Installation

You can install the package via composer:

```bash
composer require menumbing/event-stream
```

## Requirements

- PHP >= 8.1
- Hyperf Framework >= 3.1
- Redis Server

## Configuration

Publish the configuration file:

```bash
php bin/hyperf.php vendor:publish menumbing/event-stream
```

This will create a `event_stream.php` configuration file in your `config/autoload` directory.

### Configuration Options

The configuration file contains the following options:

```php
return [
    // The name of the consumer group used for stream processing
    'group_name' => env('APP_NAME', 'menumbing'),

    // Available stream drivers configuration
    'drivers' => [
        'default' => [
            'driver'  => RedisStream::class,
            'id'      => DefaultRedisId::class,
            'options' => [
                'pool'             => 'default', // Redis connection pool name
                'read_from'        => ReadMessageFrom::GROUP_CREATED, // Starting point for reading messages
                'wait_time'        => 100, // Wait time in milliseconds between read attempts
                'retention_period' => 7, // Message retention period in days
                'approx'           => false, // Use approximate for deleting messages
            ],
        ],
    ],

    // Consumer configuration settings
    'consumer' => [
        'processes' => [], // Array of process configurations for different streams
        'block_for' => 1, // The number of seconds to sleep between processing batches
        'retry_after' => 60, // Redis only: the number of seconds after which a pending message of a stopped consumer is claimed again

        // Retry in place and dead-letter queue, see "Retry and dead-letter queue" below
        'dead_letter' => [
            'enabled' => true,
            'max_attempts' => 3,
            'backoff_ms' => 500,
            'max_backoff_ms' => 5000,
            'suffix' => '.dlq',
        ],
    ],

    // Serialization configuration for messages
    'serialization' => [
        'serializer' => 'default', // The serializer service to use
        'format'     => 'json', // The format to serialize messages to
    ]
];
```

## Usage

### Producing Events

To produce events, you need to:

1. Create an event class
2. Annotate it with `#[ProducedEvent]`
3. Dispatch the event using the event dispatcher

```php
<?php

namespace App\Event;

use Menumbing\EventStream\Annotation\ProducedEvent;

#[ProducedEvent(stream: 'user-events', name: 'user.created', driver: 'default')]
class UserCreated
{
    public function __construct(
        public readonly int $userId,
        public readonly string $username,
        public readonly string $email
    ) {
    }
}
```
When the event dispatched with EventDispatcher it will be produced to event stream with name `stream-1`, event name `user.created` and driver `default`.
Here is how you dispatch the event:

```php
<?php

namespace App\Controller;

use App\Event\UserCreated;
use Hyperf\Di\Annotation\Inject;
use Psr\EventDispatcher\EventDispatcherInterface;

class UserController
{
    #[Inject]
    protected EventDispatcherInterface $eventDispatcher;

    public function create()
    {
        // Create user logic...
        $userId = 123;
        $username = 'john_doe';
        $email = 'john@example.com';

        // Dispatch event
        $this->eventDispatcher->dispatch(new UserCreated($userId, $username, $email));

        // ...
    }
}
```

### Consuming Events

To consume events, you need to:

1. Create an event class on other service
2. Annotate it with `#[ConsumedEvent]`
3. Configure it in the `event_stream.php` configuration file

Example consumer process:

```php
<?php

namespace App\Event;

use Menumbing\EventStream\Annotation\ConsumedEvent;

#[ConsumedEvent(stream: 'user-events', name: 'user.created', driver: 'default', retries: 0)]
class ConsumeUserCreated
{
    public function __construct(
        public readonly int $userId,
        public readonly string $username,
        public readonly string $email
    ) {
    }
}
```
It will automatically create a consumer process for each stream. The option `retries` indicate how many attempt should consumer process the message until it fire `ConsumeFailed` event. Alternatively, you can configure consumers in the `event_stream.php` configuration file:

```php
'consumer' => [
    'processes' => [
        'user-events' => 2, // This will create 2 processes for 'user-events' stream
    ],
    'block_for' => 1, // The number of seconds to sleep between processing batches of messages.
    'retry_after' => 30, // Redis only: the number of seconds after which a pending message of a stopped consumer is claimed again.
],
```

### Handling Consumed Events

To handle consumed events, create an event listeners for your event classes:

```php
<?php

namespace App\Listener;

use App\Event\ConsumeUserCreated;
use Hyperf\Event\Annotation\Listener;
use Hyperf\Event\Contract\ListenerInterface;

#[Listener]
class UserCreatedListener implements ListenerInterface
{
    public function listen(): array
    {
        return [
            ConsumeUserCreated::class,
        ];
    }

    public function process(object $event): void
    {
        // Handle the UserCreated event
        if ($event instanceof ConsumeUserCreated) {
            // Process the event
            echo "User created: {$event->username} ({$event->email})\n";
        }
    }
}
```

## Retry and dead-letter queue

### Why

The Kafka driver does not give a failed message back to the consumer. Its
`getIdleMessages()` yields nothing, and the next ack of a later message commits
the offset past the failed one. Before this feature, 1 exception in a listener
was a silent and permanent loss of the message on Kafka.

### Behaviour

1. The consumer calls the handler. If the handler returns ACK, the consumer
   acks the message.
2. If the handler throws or returns NACK, the consumer tries again in place,
   up to `max_attempts` times in total. The pause before the next attempt is
   `backoff_ms`, then 2 x `backoff_ms`, and so on, with a maximum of
   `max_backoff_ms`. Each failed attempt dispatches `ConsumeFailed`.
3. An exception in `skip_exceptions` does not get a retry. The consumer sends
   the message to the dead-letter stream at once. This also applies when the
   handler turns the exception into a NACK (`#[ConsumedEvent(retries: n)]`).
   A NACK without an exception is never skip-listed.
4. After the last failed attempt, the consumer publishes a copy of the message
   to the dead-letter stream, dispatches `DeadLettered`, and then acks the
   original. The copy has the same `type` and `data`, and a `dead_letter`
   block in its context:

   ```php
   'dead_letter' => [
       'source_stream' => 'user-events',
       'group' => 'billing-service',
       'message_id' => '<id of the original message>',
       'attempts' => 3,
       'failed_at' => '2026-11-04T10:15:02+00:00', // ISO-8601
       'error' => ['class' => RuntimeException::class, 'message' => '...'],
   ],
   ```

5. `DebugListener` logs 1 error line per dead-lettered message, also when
   `debug` is off: `EventStream [Dead-lettered] consumer=... group=...
   stream=... dead_letter_stream=... message_id=... type=... attempts=...
   error=...`.
6. If the publish to the dead-letter stream fails, the consumer logs
   `EventStream [Dead-letter publish failed] ... payload=...` at `critical`
   level, and then acks the original. This line contains the full message as
   JSON. It is the last copy of the message.
7. If a `DeadLettered` listener throws, the consumer logs
   `EventStream [Dead-lettered listener failed] ...` at `error` level and
   then acks the original. The consumer does not throw the listener exception
   again: the log line already records it.
8. If a `BeforeConsume`, `AfterConsume` or `ConsumeFailed` listener throws,
   the consumer logs `EventStream [Listener failed] event=... ...` at `error`
   level and continues. A listener failure does not change the result of the
   attempt, and it does not prevent the ack or the dead letter.

The consumer acks every message exactly 1 time. With longlang/phpkafka an ack
is not a mark on 1 message: it adds 1 to the fetch offset of the partition and
commits that value. A message that the consumer does not ack is not given
back. The offset only stays 1 behind, and the last message of the batch comes
back as a duplicate.

`#[ConsumedEvent(retries: n)]` continues to work. The handler NACK budget and
the attempt budget are 1 budget, and `max_attempts` is the limit. When the
handler answers NACK, the consumer uses the exception that the handler caught
as the failure. Thus the real error goes to `ConsumeFailed` and to
`dead_letter.error`, also when `n` >= `max_attempts`.
`concurrent` > 1 also works: each message has its own attempts.

The Redis driver keeps its pending-entry redelivery for a consumer that stops
before an ack. `retry_after` now applies only to that case, because the
consumer acks every message that it finishes.

### Configuration

`event_stream.consumer.dead_letter`:

| Key | Default | Effect |
|---|---|---|
| `enabled` | `true` | `false` gives the previous behaviour (1 attempt per delivery, no copy). |
| `max_attempts` | `3` | Attempts in total, including the first. |
| `backoff_ms` | `500` | The first pause, in milliseconds. It doubles per attempt. |
| `max_backoff_ms` | `5000` | The maximum of 1 pause, in milliseconds. |
| `suffix` | `.dlq` | The end of the dead-letter stream name. |

### Dead-letter stream name

`<stream>.<group><suffix>`, for example `user-events.billing-service.dlq`.
`<group>` is the group of the consumer process: `event_stream.group_name`,
unless a custom process sets its own `$groupName`.

The name contains the group because a stream is often a fan-out stream: more
than 1 service reads it, each with its own group. A dead letter belongs to the
group that failed it.

The dead-letter topics must exist on the broker before you deploy, unless the
broker creates topics automatically (`auto.create.topics.enable`). Use the same
retention as the source topic, or longer: a dead letter that the broker deletes
before a replay is lost.

### Replay

```bash
php bin/hyperf.php event-stream:dlq-replay --stream=user-events --dry-run
php bin/hyperf.php event-stream:dlq-replay --stream=user-events
php bin/hyperf.php event-stream:dlq-replay --stream=user-events --type=user.created
```

| Option | Default | Effect |
|---|---|---|
| `--stream` | (required) | The source stream, not the dead-letter name. |
| `--limit` | `100` | The maximum number of messages to replay (or hold) in 1 run. |
| `--type` | (all) | Replay only the messages with this event name. |
| `--dry-run` | off | Print only. The command does not dispatch, publish or ack. |
| `--idle-seconds` | `5` | Stop when no new message arrives for this time. |
| `--max-replays` | `3` | Hold a message that the command replayed this number of times (poison guard). |
| `--discard-poisoned` | off | Ack a held message and do not keep it. The payload goes to a `warning` log line. |
| `--driver` | `default` | The event stream driver of the source stream. |
| `--group` | `event_stream.group_name` | The consumer group whose dead letters to replay. Use it for a consumer process that sets its own `$groupName`. |

The replay is local. The command gives each message to the listeners of this
application through `ConsumerEventHandler`, in the same way as the consumer.
The command does not publish to `<stream>`: other groups read that stream, and
a copy there would run again in each of them.

The command reads `<stream>.<group><suffix>` with the group
`<group>-dlq-replay`, where `<group>` is `--group` or `event_stream.group_name`.
A dead letter with another `dead_letter.group` is foreign and is kept. Each
run starts after the last message that a real run acked.

The start position of the first run:

- Kafka: longlang/phpkafka starts a new group at offset 0. When retention has
  already deleted that offset, the client goes to the newest message and skips
  every older dead letter. To prevent this, the command reads with the Kafka
  consumer option `offset_reset => 'earliest'`: a position that is not
  retained moves to the earliest retained message. If the consumer cannot
  get the earliest offsets, it stops its heartbeat, leaves the group and
  fails the run (exit code 1). It does not fall back to the client position,
  because that position can be the newest message.
- Redis: the command creates the replay group at the start of the stream.

Retention still applies. A dead letter that the broker deletes before a real
run is lost, so run the first real replay before the oldest dead letter
expires. A dry run does not commit a position, so it does not keep a dead
letter on the stream.

For each message:

- If the listeners succeed, the command acks the message.
- If the listeners fail again, the command keeps the message on the
  dead-letter stream with `replayed_from_dlq.count` + 1 and
  `dead_letter.last_replay_error`.
- The command also keeps a message that does not match `--type`, a message
  with a type that this application does not consume, and a message with a
  `dead_letter.group` of another group.
- The command holds a message with `replayed_from_dlq.count` >= `--max-replays`.
  It prints the message with its payload and keeps it. Without
  `--discard-poisoned`, only the topic retention removes a held message.

To keep a message, the command publishes it again to the dead-letter stream
and then acks the old copy. The message moves to the end of the stream. The
run stops when it reads a message of the same lineage
(`replayed_from_dlq.original_message_id`, else `dead_letter.message_id`, else
the id) for the second time.

Limits:

- 1 Kafka fetch reads from 1 partition leader. On a dead-letter topic with
  more than 1 partition, a run with `--type` or with held messages can stop
  early. Run the command again; the next run starts where the last run stopped.
- If the publish of a kept message fails, the command stops at that message
  and does not ack it or a message after it. The exit code is 1.
- On Redis, a dry run leaves the messages pending in the replay group. The
  next run claims them first. A second dry run shows only the first pending
  message and the new messages.
- The default retry budget is about 1.5 seconds (3 attempts, pauses of 500 ms
  and 1000 ms). An outage that is longer, for example of the database, sends
  every message of that period to the dead-letter stream. The replay is the
  recovery procedure.
- The consumer runs the listeners again on each retry, and the command runs
  them again on each replay. Make the listeners idempotent.

The command is CLI-only. A console command does not start the server, so the
command registers the producers itself. Inside a server process the producers
are already registered, and a second registration would publish each produced
event 2 times. Thus the command refuses to run when
`Hyperf\Process\ProcessManager::isRunning()` is true. Do not run it from a
crontab inside the server.

If the listeners of your application need more registration at boot (for
example HTTP clients that a server-start listener registers), extend
`Menumbing\EventStream\Command\DlqReplayCommand`, override
`registerRuntime()`, and call the parent method. The command calls it 1 time
per process, before the first replay. Give the subclass its own name with
the `$name` property (for example `protected ?string $name = 'app:dlq-replay';`)
and register it as a command of your application.

### Breaking change

The dead-letter path is on by default. These things change for an existing
application:

- A message that fails all attempts goes to `<stream>.<group>.dlq`. Create the
  dead-letter topics before you deploy, unless the broker creates them.
- An exception in `skip_exceptions` sends the message to the dead-letter
  stream. Before, the consumer acked it and dropped it.
- Redis: `#[ConsumedEvent(retries: n)]` used to mean n redeliveries through
  the pending list, spaced by `retry_after` (default 60 seconds). Now it means
  at most `max_attempts` attempts about 1.5 seconds apart, then the
  dead-letter stream. An outage of 2 minutes that the consumer rode out before
  now sends the messages of that period to the dead-letter stream. Replay
  them, or increase `max_attempts` and `max_backoff_ms`.
- Redis: a NACK no longer leaves the entry pending for `retry_after`. The
  consumer acks the message after the last attempt. `retry_after` now matters
  only for a consumer that stops in the middle of a message.
- The package now requires `hyperf/command` and `hyperf/context`.

Set `event_stream.consumer.dead_letter.enabled` to `false` to get the previous
behaviour.

## Debugging

The package includes a debug listener that logs various events when debugging is enabled. To enable debugging, set `app_debug` to `true` in your configuration.

## Advanced Usage

### Custom ID Providers

You can create custom ID providers by implementing the `IdProviderInterface`:

```php
<?php

namespace App\EventStream;

use Menumbing\Contract\EventStream\IdProviderInterface;

class CustomIdProvider implements IdProviderInterface
{
    public function newId(): string
    {
        // Generate and return a custom ID
        return uniqid('custom-', true);
    }
}
```

Then configure it in your `event_stream.php`:

```php
'drivers' => [
    'custom' => [
        'driver'  => RedisStream::class,
        'id'      => CustomIdProvider::class,
        'options' => [
            // ...
        ],
    ],
],
```

### Custom Stream Drivers

You can create custom stream drivers by implementing the `StreamInterface`:

```php
<?php

namespace App\EventStream;

use Generator;
use Menumbing\Contract\EventStream\StreamInterface;
use Menumbing\Contract\EventStream\StreamMessage;

class CustomStreamDriver implements StreamInterface
{
    public function createGroup(string $name, string $stream): bool
    {
        // Implementation
    }

    public function publish(StreamMessage $message): string
    {
        // Implementation
    }

    public function subscribe(string $consumer, string $group, array $streams): Generator
    {
        // Implementation
    }

    public function ack(string $group, string $stream, array $ids): bool
    {
        // Implementation
    }
}
```

Then configure it in your `event_stream.php`:

```php
'drivers' => [
    'custom' => [
        'driver'  => CustomStreamDriver::class,
        'options' => [
            // Custom options
        ],
    ],
],
```

## License

The MIT License (MIT). Please see [License File](LICENSE) for more information.
