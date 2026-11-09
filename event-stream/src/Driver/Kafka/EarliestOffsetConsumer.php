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
 * longlang/phpkafka jumps to LATEST when a group's offset is no longer retained (e.g. offset 0 of a
 * fresh group after the first segment expired). After each join, this moves such positions to EARLIEST.
 *
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */
class EarliestOffsetConsumer extends Consumer
{
    public const EARLIEST_TIMESTAMP = -2;

    public function rejoin(): void
    {
        $this->joinGroup();

        try {
            $this->seekToEarliestWhereNotRetained();
        } catch (Throwable $e) {
            // The join started the heartbeat: stop it, or a joined member stays behind with a live timer.
            // Rethrown, not swallowed: the library fallback could start at the newest message.
            $this->stopHeartbeat();

            try {
                $this->close();
            } catch (Throwable) {
            }

            throw $e;
        }
    }

    /**
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

            foreach (self::stepsToEarliest($positions, $earliest) as $partition => $step) {
                $offsetManager->addFetchOffset($partition, $step);
            }
        }
    }

    /**
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
