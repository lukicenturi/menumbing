<?php

declare(strict_types=1);

namespace Menumbing\EventStream\Driver\Kafka;

use longlang\phpkafka\Consumer\Consumer;
use longlang\phpkafka\Consumer\OffsetManager;
use longlang\phpkafka\Protocol\ErrorCode;
use longlang\phpkafka\Protocol\ListOffset\ListOffsetPartition;
use longlang\phpkafka\Protocol\ListOffset\ListOffsetRequest;
use longlang\phpkafka\Protocol\ListOffset\ListOffsetResponse;
use longlang\phpkafka\Protocol\ListOffset\ListOffsetTopic;
use longlang\phpkafka\Util\KafkaUtil;
use Throwable;

/**
 * A consumer that starts at the earliest retained message when its position
 * is no longer retained, instead of at the newest one.
 *
 * longlang/phpkafka starts a group at `max(committed, 0)`. If retention has
 * already deleted that offset, the fetch answers OFFSET_OUT_OF_RANGE, and the
 * client then calls `OffsetManager::updateListOffsets()`, which only asks for
 * the LATEST offset (timestamp -1): the consumer jumps to the end of the
 * partition and every older retained message is skipped without a trace. For
 * a fresh group on a topic whose first segment expired, that is every message.
 *
 * So after each (re)join, before the first fetch, this consumer asks for the
 * EARLIEST offset (timestamp -2) of each partition and moves a position that
 * is below it up to it. A position that is still retained does not change.
 *
 * Selected with the consumer option `offset_reset => 'earliest'`.
 *
 * If that seek fails, the consumer stops its heartbeat, leaves the group and
 * throws: it does not fall back to the library position.
 *
 * Limit: a segment that expires while this consumer reads it still triggers
 * the library's jump to the end, because that fallback runs inside the
 * private fetch loop.
 *
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */
class EarliestOffsetConsumer extends Consumer
{
    /** ListOffsets timestamp for the log start offset. */
    public const EARLIEST_TIMESTAMP = -2;

    public function rejoin(): void
    {
        $this->joinGroup();

        try {
            $this->seekToEarliestWhereNotRetained();
        } catch (Throwable $e) {
            // The join already started the heartbeat timer. Left alone, a
            // failed seek would leave a joined member with a live timer behind
            // (one per failed subscribe round, and FENCED_INSTANCE_ID on later
            // ticks). Rethrown, not swallowed: falling back to the library
            // position could jump to the newest message, and a wrong start
            // position is worse than a failed run. The caller builds a new
            // consumer on its next round.
            $this->stopHeartbeat();

            try {
                $this->close();
            } catch (Throwable) {
                // Leaving the group is best effort; the error that matters is $e.
            }

            throw $e;
        }
    }

    /**
     * The step to add to each position that is below the earliest retained
     * offset of its partition, keyed by partition. Positions that are still
     * retained, and partitions without an earliest offset, are left out.
     *
     * @param  array<int, int>  $positions  partition => fetch offset
     * @param  array<int, int>  $earliest  partition => earliest retained offset
     *
     * @return array<int, int> partition => step (> 0)
     */
    public static function stepsToEarliest(array $positions, array $earliest): array
    {
        $steps = [];

        foreach ($positions as $partition => $position) {
            if (isset($earliest[$partition]) && $position < $earliest[$partition]) {
                $steps[$partition] = $earliest[$partition] - $position;
            }
        }

        return $steps;
    }

    /**
     * The library join; a seam so the order (join first, then seek) is testable.
     */
    protected function joinGroup(): void
    {
        parent::rejoin();
    }

    protected function seekToEarliestWhereNotRetained(): void
    {
        foreach ($this->offsetManagers as $topic => $offsetManager) {
            /** @var OffsetManager $offsetManager */
            $positions = $offsetManager->getOffsets();

            if ([] === $positions) {
                continue;
            }

            $earliest = $this->listEarliestOffsets((string) $topic, array_keys($positions));

            // OffsetManager keeps its positions private; addFetchOffset() is
            // the public way to move one.
            foreach (self::stepsToEarliest($positions, $earliest) as $partition => $step) {
                $offsetManager->addFetchOffset($partition, $step);
            }
        }
    }

    /**
     * The same request as `OffsetManager::updateListOffsets()`, with the
     * EARLIEST timestamp, and the offsets returned instead of applied.
     *
     * @param  int[]  $partitions
     *
     * @return array<int, int> partition => earliest retained offset
     */
    protected function listEarliestOffsets(string $topic, array $partitions): array
    {
        $broker = $this->broker;
        $topicsMeta = $broker->getTopicsMeta($topic);
        $byBroker = [];

        foreach ($partitions as $partition) {
            $brokerId = $broker->getBrokerIdByTopic($topic, $partition);

            // No known leader: null when the partition is not in the metadata,
            // -1 (the metadata leader id) when it has no leader right now.
            // Leave that partition on the library's own position rather than
            // asking a broker that does not exist.
            if (null === $brokerId || $brokerId < 0) {
                continue;
            }

            $byBroker[$brokerId][] = $partition;
        }

        $earliest = [];

        foreach ($byBroker as $brokerId => $brokerPartitions) {
            $requestPartitions = [];

            foreach ($brokerPartitions as $partition) {
                $requestPartition = (new ListOffsetPartition())->setPartitionIndex($partition)->setTimestamp(self::EARLIEST_TIMESTAMP);

                foreach ($topicsMeta as $topicMeta) {
                    if ($topicMeta->getName() !== $topic) {
                        continue;
                    }

                    foreach ($topicMeta->getPartitions() as $partitionMeta) {
                        if ($partitionMeta->getPartitionIndex() === $partition) {
                            $requestPartition->setCurrentLeaderEpoch($partitionMeta->getLeaderEpoch());
                        }
                    }
                }

                $requestPartitions[] = $requestPartition;
            }

            $request = (new ListOffsetRequest())->setTopics([
                (new ListOffsetTopic())->setName($topic)->setPartitions($requestPartitions),
            ]);

            /** @var ListOffsetResponse $response */
            $response = KafkaUtil::retry($broker->getClientByBrokerId((int) $brokerId), $request, $this->config->getOffsetRetry(), 0);

            foreach ($response->getTopics() as $responseTopic) {
                foreach ($responseTopic->getPartitions() as $responsePartition) {
                    if (ErrorCode::success($responsePartition->getErrorCode())) {
                        $earliest[$responsePartition->getPartitionIndex()] = $responsePartition->getOffset();
                    }
                }
            }
        }

        return $earliest;
    }
}
