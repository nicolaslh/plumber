<?php

/**
 *  通过workerman作为常驻进程，拉取消费队列数据， 使用回调处理业务逻辑， 仅作为队列消费的中间件，不干涉本身业务。
 *
 */

namespace Plumber\Plumber;
//require_once __DIR__ . '/../../Workerman/Autoloader.php';
use FFI\Exception;
use Plumber\Plumber\Provider;
use Plumber\Plumber\Provider\SQS;
use Plumber\Plumber\Provider\RabbitMQ;
use Workerman\Worker;

class plumber
{

    public static $callback;
    public $queueName;
    /**
     * 队列提供商
     * @var string
     */
    public $provider;

    /**
     * 设置当前Worker实例启动多少个进程，不设置时默认为1。
     * @var integer
     */
    public $count;

    /**
     * Summary of $config
     * @provider string 支持的供应商
     * @credential array
     * * @key string 账号
     * * @secret string 秘钥
     * @var mixed
     */
    public $config;
    public $consumer;
    public $_queueClient;


    public function __construct($queueName, $callback, $config = [])
    {
        $this->queueName = $queueName;
        self::$callback = $callback;
        if (empty($config["provider"])) {
            throw new \Exception("provider can't empty");
        }
        if (!in_array($config["provider"], define::PROVIDER_SUPPORT)) {
            throw new \Exception($config["provider"] . " is not support");
        }
        $this->provider = $config["provider"];
        $this->config = $config;
        $consumer = new Worker();
        $this->consumer = $consumer;
        $this->getQueueClient();
    }

    public function Run()
    {
        if (isset($this->config["count"]) && $this->config["count"] > 1) {
            $this->setProcess($this->config["count"]);
        }
        $this->consumer->onWorkerStart = function ($consumer) {
            $this->Receiver();
        };
        Worker::runAll();
    }

    /**
     * 设置进程数量
     * @param $count
     */
    public function setProcess($count)
    {
        $this->consumer = $count;
    }

    public function Receiver()
    {
        switch ($this->provider) {
            case define::RabbitMQ:
                print_r("RabbitMQ comsuming...");
                $data = $this->_queueClient->receive($this->queueName, "Plumber\Plumber\plumber::HandleAMQCallback");
                break;
            case define::Kafka:
                print_r("Kafka comsuming...");
                $data = $this->_queueClient->receive();
                if (empty($data)) {
                    continue;
                }
                $result = $this->ExecCallback($data);
                print_r(self::$callback. " exec result: ". json_encode($result));
                if (isset($result["msg"]) && $result["msg"] == "success") {
                    // todo ack
                }
                break;
            case define::SQS:
            default:
                print_r("SQS comsuming...");
                while (1) {
                    $data = $this->_queueClient->receive();
                    if (!$data) {
                        continue;
                    }
                    $result = $this->ExecCallback($data["Body"]);
                    print_r(self::$callback. " exec result: ". json_encode($result));
                    if (isset($result["msg"]) && $result["msg"] == "success") {
                        $this->_queueClient->delete($data["ReceiptHandle"]);
                    }
                }
                break;
        }
    }

    public static function HandleAMQCallback($amqRawData)
    {
        $body = $amqRawData->getBody();
        print_r("receive data: {$body}");
        $result = call_user_func(self::$callback, $body);
        print_r(self::$callback. " exec result: ". json_encode($result));
        if (isset($result["msg"]) && $result["msg"] == "success") {
            $amqRawData->ack();
        }
    }

    public function ExecCallback($data)
    {
        print_r("receive data: {$data}");
        return call_user_func(self::$callback, $data);
    }

    public function getQueueClient()
    {
        switch ($this->provider) {
            case define::RabbitMQ:
                $this->_queueClient = new RabbitMQ($this->config);
                break;
            case define::Kafka:
                $this->_queueClient = new RabbitMQ($this->config);
                break;
            case define::SQS:
            default:
                $this->_queueClient = new SQS($this->queueName, $this->config);
                break;
        }

        if (empty($this->_queueClient)) {
            throw new \Exception($this->provider . " client initial fail");
        }
    }

    public function removeQueueData($message)
    {
        return $this->_queueClient->delete($message);
    }

    // public function __destruct()
    // {
    //     $this->_queueClient->shutdown();
    // }
}
