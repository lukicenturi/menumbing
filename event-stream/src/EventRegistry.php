<?php

declare(strict_types=1);

namespace Menumbing\EventStream;

use Hyperf\Di\Annotation\AnnotationCollector;
use Menumbing\EventStream\Annotation\ConsumedEvent;
use Menumbing\EventStream\Exception\EventNameConflictedException;
use Menumbing\EventStream\Exception\EventNotFoundException;

/**
 * @author  Iqbal Maulana <iq.bluejack@gmail.com>
 */
class EventRegistry
{
    protected array $events = [];

    protected bool $initialized = false;

    public function init(): void
    {
        $classes = AnnotationCollector::getClassesByAnnotation(ConsumedEvent::class) ?? [];

        /**
         * @var string $class
         * @var ConsumedEvent $annotation
         */
        foreach ($classes as $class => $annotation) {
            $name = $annotation->name;
            if ($this->has($name)) {
                throw new EventNameConflictedException($name);
            }

            $this->events[$name] = [
                'class' => $class,
                'retries' => $annotation->retries,
            ];
        }

        $this->initialized = true;
    }

    /**
     * Fills the registry unless that already happened.
     *
     * In a server, ConsumerManager::register() fills it at boot, and init()
     * throws on a second fill (every name would conflict with itself). A
     * console command never reaches that boot, so code that can run in both
     * places calls this instead.
     */
    public function ensureInitialized(): void
    {
        if (!$this->initialized) {
            $this->init();
        }
    }

    public function getClassByName(string $name): array
    {
        if (!$this->has($name)) {
            throw new EventNotFoundException(sprintf('Event "%s" not found.', $name));
        }

        return $this->events[$name];
    }

    public function has(string $name): bool
    {
        return array_key_exists($name, $this->events);
    }
}
