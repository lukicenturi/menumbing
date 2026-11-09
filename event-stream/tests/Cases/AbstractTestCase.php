<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Tests\Cases;

use Hyperf\Config\Config;
use Hyperf\Context\ApplicationContext;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Di\Annotation\AnnotationCollector;
use Hyperf\Di\Container;
use Hyperf\Di\Definition\DefinitionSource;
use Hyperf\Process\ProcessManager;
use Menumbing\EventStream\EventRegistry;
use Menumbing\EventStream\Factory\StreamFactory;
use Menumbing\EventStream\Tests\Support\FakeEventStream;
use Menumbing\EventStream\Tests\Support\SpyEventDispatcher;
use Menumbing\EventStream\Tests\Support\SpyStdoutLogger;
use Menumbing\Serializer\Factory\SerializerFactory;
use PHPUnit\Framework\TestCase;
use Psr\EventDispatcher\EventDispatcherInterface;
use ReflectionProperty;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Serializer\Normalizer\ObjectNormalizer;
use Symfony\Component\Serializer\Serializer;

/**
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */
abstract class AbstractTestCase extends TestCase
{
    protected const GROUP = 'billing-service';

    protected Container $container;

    protected Config $config;

    protected FakeEventStream $stream;

    protected SpyEventDispatcher $events;

    protected SpyStdoutLogger $logger;

    protected function setUp(): void
    {
        parent::setUp();

        $eventStream = require dirname(__DIR__, 2) . '/publish/event_stream.php';
        $eventStream['group_name'] = self::GROUP;

        $this->config = new Config([
            'event_stream' => $eventStream,
            'serializer' => [
                'default' => [
                    'normalizers' => [ObjectNormalizer::class],
                    'encoders' => [JsonEncoder::class],
                ],
            ],
        ]);
        $this->stream = new FakeEventStream();
        $this->events = new SpyEventDispatcher();
        $this->logger = new SpyStdoutLogger();

        $streamFactory = new StreamFactory($this->config, new SerializerFactory($this->config));
        (new ReflectionProperty(StreamFactory::class, 'streams'))->setValue($streamFactory, ['default' => $this->stream]);

        $this->container = new Container(new DefinitionSource([]));
        $this->container->set(ConfigInterface::class, $this->config);
        $this->container->set(StreamFactory::class, $streamFactory);
        $this->container->set(EventDispatcherInterface::class, $this->events);
        $this->container->set(StdoutLoggerInterface::class, $this->logger);
        $this->container->set(Serializer::class, new Serializer([new ObjectNormalizer()], [new JsonEncoder()]));
        $this->container->set(EventRegistry::class, new EventRegistry());
        ApplicationContext::setContainer($this->container);
    }

    protected function tearDown(): void
    {
        AnnotationCollector::clear();
        ProcessManager::setRunning(false);

        parent::tearDown();
    }
}
