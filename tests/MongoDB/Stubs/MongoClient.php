<?php

namespace MongoDB\Stubs;

class MongoClient extends \MongoClient
{
    private static $connections = array();

    public function __construct()
    {
    }

    public static function getConnections()
    {
        return self::$connections;
    }

    public static function setConnections(array $connections)
    {
        self::$connections = $connections;
    }
}
