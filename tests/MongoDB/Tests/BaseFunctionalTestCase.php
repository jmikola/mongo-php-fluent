<?php

namespace MongoDB\Tests;

use MongoClient;

class BaseFunctionalTestCase extends BaseTestCase
{
    const CONN_STANDALONE = 0x01;
    const CONN_PRIMARY = 0x02;
    const CONN_SECONDARY = 0x04;
    const CONN_ARBITER = 0x08;
    const CONN_MONGOS = 0x10;

    const PHP_MONGO_API_WRITE_API = 2;

    protected static $client;

    public function setUp()
    {
        $this->getMongoCollection()->drop();
    }

    public static function setUpBeforeClass()
    {
        $uri = isset($_ENV['MONGODB_URI']) ? $_ENV['MONGODB_URI'] : 'mongodb://localhost:27017';

        self::$client = new MongoClient($uri);
    }

    protected function getMongoClient()
    {
        return self::$client;
    }

    protected function getMongoCollection()
    {
        return $this->getMongoClient()->selectCollection($this->getDatabase(), $this->getCollection());
    }

    protected function getMongoDB()
    {
        return $this->getMongoClient()->selectDB($this->getDatabase());
    }

    protected function requiresLegacyWriteOperations()
    {
        if ($this->isWriteApiSupported()) {
            $this->markTestSkipped('Legacy write operations are not available.');
        }
    }

    protected function requiresWriteCommands()
    {
        if ( ! $this->isWriteApiSupported()) {
            $this->markTestSkipped('Write commands are not available.');
        }
    }

    /**
     * Checks if the client's writable connection(s) support write commands.
     *
     * @return boolean
     */
    private function isWriteApiSupported()
    {
        $client = $this->getMongoClient();
        $writable = (self::CONN_STANDALONE | self::CONN_PRIMARY | self::CONN_MONGOS);

        foreach ($client->getConnections() as $connection) {
            $connection = $connection['connection'];

            if ( ! ($connection['connection_type'] & $writable)) {
                continue;
            }

            if ($connection['min_wire_version'] > self::PHP_MONGO_API_WRITE_API) {
                continue;
            }

            if ($connection['max_wire_version'] < self::PHP_MONGO_API_WRITE_API) {
                continue;
            }

            return true;
        }

        return false;
    }
}