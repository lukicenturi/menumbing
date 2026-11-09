<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://hyperf.wiki
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */
namespace Menumbing\EventStream\Tests\Support;

use Hyperf\Contract\StdoutLoggerInterface;
use Psr\Log\AbstractLogger;
use Stringable;

/**
 * Keeps every log call with its placeholders filled in, the way the stdout
 * logger prints them.
 */
final class SpyStdoutLogger extends AbstractLogger implements StdoutLoggerInterface
{
    /** @var list<array{level: string, message: string}> */
    public array $records = [];

    public function log($level, string|Stringable $message, array $context = []): void
    {
        $replace = [];
        foreach ($context as $key => $value) {
            if (is_scalar($value) || $value instanceof Stringable) {
                $replace['{' . $key . '}'] = (string) $value;
            }
        }

        $this->records[] = ['level' => (string) $level, 'message' => strtr((string) $message, $replace)];
    }

    /** @return list<string> */
    public function messages(string $level): array
    {
        return array_values(array_map(
            static fn (array $record): string => $record['message'],
            array_filter($this->records, static fn (array $record): bool => $record['level'] === $level),
        ));
    }
}
