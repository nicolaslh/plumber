<?php

namespace Plumber\Plumber\Provider;

use RdKafka\Conf;
use RdKafka\Message;

/**
 * 高级消费者
 * Class HighLevelConsumer
 * @package App\Library\Kafka
 */
class Kafka
{
    /**
     * @var \RdKafka\KafkaConsumer
     */
    private $consumer;

    /**
     * 消费开始的offset
     */
    const BEGIN = 'earliest';

    /**
     * 分组名
     */
    private $group_id;

    /**
     * 当前消费到的位置
     */
    private $offset;

    /**
     * 是否自动提交offset
     * @var int
     */
    private $auto_commit;

    /**
     * 自动提交offset时间毫秒
     * @var int
     */
    private $auto_commit_interval_ms;

    /**
     * Kafka服务器地址
     */
    private $metadata_broker_list;

    /**
     * 主题数组
     */
    private $topics;

    /**
     * 构造函数
     * @param string $metadata_broker_list 服务器地址
     * @param string $group_id 分组名
     * @param array $topics 主题数组
     * @param string|null $offset 消费偏移量
     * @throws \RdKafka\Exception
     */
    public function __construct($config)
    {

        $this->metadata_broker_list = $config["kafka"]["metadata_broker_list"];
        $this->group_id = $config["kafka"]["group_id"];
        $this->topics = $config["kafka"]["topics"];
        $this->auto_commit = $config["kafka"]["auto_commit"] ?? 0;
        $this->auto_commit_interval_ms = $config["kafka"]["auto_commit_interval_ms"] ?? 5000;
        $this->offset = $config["kafka"]["offset"] ?? self::BEGIN;

        //初始化高级消费者
        $this->init();
    }

    /**
     * 初始化
     * @throws \RdKafka\Exception
     */
    private function init()
    {

        $conf = new Conf();

        // Set a rebalance callback to log partition assignments (optional)
        $conf->setRebalanceCb(function (\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
            /**
             * @var \RdKafka\TopicPartition[] $partitions
             */
            $partitionStr = json_encode(array_map(function (\RdKafka\TopicPartition $partition) {
                return [
                    'topic' => $partition->getTopic(),
                    'partition' => $partition->getPartition(),
                    'offset' => $partition->getOffset(),
                ];
            }, $partitions));

            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    info(__METHOD__ . ' rebalance assign callback：' . $partitionStr);
                    $kafka->assign($partitions);
                    break;

                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    info(__METHOD__ . ' rebalance revoke callback：' . $partitionStr);
                    $kafka->assign(NULL);
                    break;

                default:
                    error(__METHOD__ . ' rebalance unknown callback：' . $err);
                    throw new \Exception($err);
            }
        });

        // Configure the group.id. All consumer with the same group.id will consume
        // different partitions.
        $conf->set('group.id', $this->group_id);

        // Initial list of Kafka brokers
        $conf->set('metadata.broker.list', $this->metadata_broker_list);

        // Set where to start consuming messages when there is no initial offset in
        // offset store or the desired offset is out of range.
        // 'earliest': start from the beginning
        $conf->set('auto.offset.reset', $this->offset);

        //自动提交offset开关 和 自动提交时间间隔
        $conf->set('enable.auto.commit', $this->auto_commit);
        $conf->set('auto.commit.interval.ms', $this->auto_commit_interval_ms);

        $this->consumer = new \RdKafka\KafkaConsumer($conf);

        // Subscribe to topic 'test'
        $this->consumer->subscribe($this->topics);
    }

    /**
     * 真实消费
     * @return Message|null
     * @throws \RdKafka\Exception
     */
    public function receive(): ?Message
    {
        $message = $this->consumer->consume(120 * 1000);
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                info(__METHOD__ . ' messages : ' . json_encode($message, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE));
                return $message;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                info(__METHOD__ . ' No more messages : ' . $message->errstr());
                return null;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                info(__METHOD__ . ' Timeout :' . $message->errstr() . ' code:' . $message->err . PHP_EOL);
                return null;
            default:
                error(__METHOD__ . ' Exception :' . $message->errstr() . ' code:' . $message->err . PHP_EOL);
                throw new \Exception(__METHOD__ . ' ' . $message->errstr(), $message->err);
        }
    }

    /**
     * 同步提交offset
     * @throws \RdKafka\Exception
     */
    public function commit(): void
    {
        $this->consumer->commit();
    }

    /**
     * 异步提交offset
     * @throws \RdKafka\Exception
     */
    public function commitAsync(): void
    {
        $this->consumer->commitAsync();
    }
}
