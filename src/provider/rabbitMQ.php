<?php

namespace Plumber\Plumber\Provider;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Connection;

class RabbitMQ
{

    protected $channel;
    protected $connection;

    public function __construct($config)
    {
        $this->connection = new AMQPStreamConnection(
            $config['rabbitmq']['host'],
            $config['rabbitmq']['port'],
            $config['rabbitmq']['login'],
            $config['rabbitmq']['password'],
            $config['rabbitmq']['vhost']
        );
        $this->channel = $this->connection->channel();
    }


    /**
     * 
     * $callback = function ($msg) {
     *      echo " [x] Received ", $msg->body, "\n";
     * };
     * @param mixed $queueName
     * @param mixed $callback
     * @return void
     */
    public function receive($queueName, $callback)
    {
        $this->channel->queue_declare($queueName, false, false, false, false);
        $this->channel->basic_consume($queueName, '', false, true, false, false, $callback);
    }

    /**
     * @param \PhpAmqpLib\Message\AMQPMessage $message
     * @param mixed $message
     * @return void
     */
    public function delete($message)
    {
        return $message->ack();
    }

    public function shutdown()
    {
        $this->channel->close();
        $this->connection->close();
    }
}
