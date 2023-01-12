<?php

namespace Plumber\Plumber;

class define
{
    const SQS = "SQS";
    const RabbitMQ = "RabbitMQ";

    const PROVIDER_SUPPORT = [
        self::SQS,
        self::RabbitMQ,
    ];
}
