<?php

declare(strict_types=1);

namespace Menumbing\EventStream\DeadLetter;

use Hyperf\Contract\ConfigInterface;

/**
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */
final class DeadLetterOptions
{
    public const DEFAULT_MAX_ATTEMPTS = 3;

    public const DEFAULT_BACKOFF_MS = 500;

    public const DEFAULT_MAX_BACKOFF_MS = 5000;

    public function __construct(
        public readonly bool $enabled = true,
        public readonly int $maxAttempts = self::DEFAULT_MAX_ATTEMPTS,
        public readonly int $backoffMs = self::DEFAULT_BACKOFF_MS,
        public readonly int $maxBackoffMs = self::DEFAULT_MAX_BACKOFF_MS,
        public readonly string $suffix = DeadLetterStream::DEFAULT_SUFFIX,
    ) {
    }

    public static function fromConfig(ConfigInterface $config): self
    {
        $options = (array) $config->get('event_stream.consumer.dead_letter', []);
        $suffix = (string) ($options['suffix'] ?? DeadLetterStream::DEFAULT_SUFFIX);

        return new self(
            enabled: (bool) ($options['enabled'] ?? true),
            maxAttempts: max(1, (int) ($options['max_attempts'] ?? self::DEFAULT_MAX_ATTEMPTS)),
            backoffMs: max(0, (int) ($options['backoff_ms'] ?? self::DEFAULT_BACKOFF_MS)),
            maxBackoffMs: max(0, (int) ($options['max_backoff_ms'] ?? self::DEFAULT_MAX_BACKOFF_MS)),
            suffix: '' === $suffix ? DeadLetterStream::DEFAULT_SUFFIX : $suffix,
        );
    }

    public function streamFor(string $sourceStream, string $group): string
    {
        return DeadLetterStream::nameFor($sourceStream, $group, $this->suffix);
    }

    public function backoffAfter(int $failedAttempts): int
    {
        $exponent = min(max(0, $failedAttempts - 1), 30);

        return (int) min($this->maxBackoffMs, $this->backoffMs * (2 ** $exponent));
    }
}
