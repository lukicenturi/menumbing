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
namespace Menumbing\EventStream;

use Menumbing\Contract\EventStream\StreamInterface;
use Menumbing\EventStream\Command\DlqReplayCommand;
use Menumbing\EventStream\Factory\StreamFactory;
use Menumbing\EventStream\Listener\DebugListener;
use Menumbing\EventStream\Listener\RegisterConsumers;
use Menumbing\EventStream\Listener\RegisterProducers;
use Psr\Container\ContainerInterface;

class ConfigProvider
{
    public function __invoke(): array
    {
        return [
            'dependencies' => [
                StreamFactory::class => StreamFactory::class,
                StreamInterface::class => fn(ContainerInterface $container) => $container->get(StreamFactory::class)->get('default'),
            ],
            'commands' => [
                DlqReplayCommand::class,
            ],
            'listeners' => [
                RegisterProducers::class,
                RegisterConsumers::class => 99,
                DebugListener::class,
            ],
            'annotations' => [
                'scan' => [
                    'paths' => [
                        __DIR__,
                    ],
                ],
            ],
            'publish' => [
                [
                    'id' => 'config',
                    'description' => 'The config for event stream.',
                    'source' => __DIR__ . '/../publish/event_stream.php',
                    'destination' => BASE_PATH . '/config/autoload/event_stream.php',
                ],
            ]
        ];
    }
}
