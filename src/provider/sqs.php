<?php

namespace Plumber\Plumber\Provider;

use app\models\Service;
use Aws\Sqs\Exception\SqsException;
use Exception;
use Yii;
use Aws\Sqs\SqsClient;

class SQS
{
    /**
     * The name of the SQS queue
     *
     * @var string
     */
    protected $name;

    /**
     * The url of the SQS queue
     *
     * @var string
     */
    protected $url;

    /**
     * The array of credentials used to connect to the AWS API
     *
     * @var array
     */
    protected $aws_credentials;

    /**
     * A SqsClient object from the AWS SDK, used to connect to the AWS SQS API
     *
     * @var SqsClient
     */
    protected $sqs_client;


    public function __construct($config = [])
    {
        $aws_credentials = self::_config($config);
        $this->sqs_client = new SqsClient($aws_credentials);
    }

    public function DeleteSqsQueue()
    {
        try {
            $url = $this->sqs_client->getQueueUrl(['QueueName' => $this->name])->get('QueueUrl');
            $this->sqs_client->deleteQueue(array(
                'QueueUrl' => $url
            ));
            return true;
        } catch (SqsException $e) {
            echo 'Error DeleteSqsQueue message to queue ' . $e->getMessage();
            return false;
        }
    }


    /**
     * @param $MaxNumberOfMessages
     * Receives a message from the queue and puts it into a Message object
     *
     * @return bool|Message  Message object built from the queue, or false if there is a problem receiving message
     */
    public function receive($MaxNumberOfMessages = 1)
    {
        try {
            // Receive a message from the queue
            $url = $this->sqs_client->getQueueUrl(array('QueueName' => $this->name))->get('QueueUrl');
            $result = $this->sqs_client->receiveMessage(array(
                'QueueUrl' => $url,
                'WaitTimeSeconds' => 20,
            ));

            if ($result['Messages'] == null) {
                // No message to process
                return false;
            }

            // Get the message and return it
            $result_message = array_pop($result['Messages']);
            //$queue_handle = $result_message['ReceiptHandle'];
            //$message_json = $result_message['Body'];
            return $result_message;
        } catch (Exception $e) {
            echo 'Error receiving message from queue ' . $e->getMessage();
            return false;
        }
    }

    /**
     * Deletes a message from the queue
     *
     * @param Message $message
     * @return bool  returns true if successful, false otherwise
     */
    public function delete($receipt_handle)
    {
        try {
            // Delete the message
            $url = $this->sqs_client->getQueueUrl(array('QueueName' => $this->name))->get('QueueUrl');
            $this->sqs_client->deleteMessage(array(
                'QueueUrl' => $url,
                'ReceiptHandle' => $receipt_handle
            ));

            return true;
        } catch (Exception $e) {
            echo 'Error deleting message from queue ' . $e->getMessage();
            return false;
        }
    }



    /**
     * Summary of _config
     * if `key` and `secret` are not exist, will use aws IAM role credential.
     * @param mixed $config
     * @return array
     */
    protected static function _config($config)
    {
        $region = $config["region"] ?? "ap-southeast-1";
        $sqsversion = $config["sqsversion"] ?? "latest";
        $awskey = $config["key"] ?? "";
        $secret = $config["secret"] ?? "";

        $aws_credentials = array(
            'region' => $region,
            'version' => $sqsversion,
        );
        if (!empty($awskey) && !empty($secret)) {
            $aws_credentials["credentials"] = array(
                'key' => $awskey,
                'secret' => $secret,
            );
        }

        return $aws_credentials;
    }

    public function shutdown()
    {
    }
}
