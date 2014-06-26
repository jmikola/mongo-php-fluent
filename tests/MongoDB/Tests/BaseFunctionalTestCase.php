<?php

namespace MongoDB\Tests;

use MongoClient;

abstract class BaseFunctionalTestCase extends BaseTestCase
{
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

    protected function assertCollectionContains(array $expectedDocuments)
    {
        $collection = $this->getMongoCollection();
        $documents = iterator_to_array($collection->find(), false);

        $this->assertSame($expectedDocuments, $documents);
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

    protected function loadIntoCollection(array $documents)
    {
        $collection = $this->getMongoCollection();

        foreach ($documents as $document) {
            $collection->insert($document);
        }
    }

    protected function requiresNoJournal()
    {
        if ($this->isJournaled()) {
            $this->markTestSkipped('Journaling is enabled.');
        }
    }

    protected function requiresLegacyWriteOperations()
    {
        if ($this->isWriteApiSupported()) {
            $this->markTestSkipped('Legacy write operations are not available.');
        }
    }

    protected function requiresReplicaSet()
    {
        if ( ! $this->isReplicaSet()) {
            $this->markTestSkipped('Replica set is not available.');
        }
    }

    protected function requiresShardCluster()
    {
        if ( ! $this->isShardCluster()) {
            $this->markTestSkipped('Shard cluster is not available.');
        }
    }

    protected function requiresStandalone()
    {
        if ($this->isReplicaSet() || $this->isShardCluster()) {
            $this->markTestSkipped('Standalone is not available.');
        }
    }

    protected function requiresWriteCommands()
    {
        if ( ! $this->isWriteApiSupported()) {
            $this->markTestSkipped('Write commands are not available.');
        }
    }

    /**
     * Checks if the client's writable connection(s) has journaling enabled.
     *
     * @return boolean
     */
    private function isJournaled()
    {
        $serverStatus = $this->getMongoDB()->command(array('serverStatus' => 1));

        return ( ! empty($serverStatus['dur']));
    }

    /**
     * Checks if the client is connected to a replica set primary.
     *
     * @return boolean
     */
    private function isReplicaSet()
    {
        $isMaster = $this->getMongoDB()->command(array('isMaster' => 1));

        return ( ! empty($isMaster['setName']));
    }

    /**
     * Checks if the client is connected to a mongos instance.
     *
     * @return boolean
     */
    private function isShardCluster()
    {
        $isMaster = $this->getMongoDB()->command(array('isMaster' => 1));

        return (isset($isMaster['msg']) && $isMaster['msg'] === 'isdbgrid');
    }

    /**
     * Checks if the client's writable connection(s) support write commands.
     *
     * @return boolean
     */
    private function isWriteApiSupported()
    {
        $isMaster = $this->getMongoDB()->command(array('isMaster' => 1));

        if (isset($isMaster['minWireVersion']) && $isMaster['minWireVersion'] <= self::PHP_MONGO_API_WRITE_API &&
            isset($isMaster['maxWireVersion']) && $isMaster['maxWireVersion'] >= self::PHP_MONGO_API_WRITE_API) {

            return true;
        }

        return false;
    }
}
