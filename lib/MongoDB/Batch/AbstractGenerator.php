<?php

namespace MongoDB\Batch;

use MongoDB\Batch\Legacy\LegacyDeleteBatch;
use MongoDB\Batch\Legacy\LegacyInsertBatch;
use MongoDB\Batch\Legacy\LegacyUpdateBatch;
use InvalidArgumentException;
use Iterator;
use MongoClient;
use MongoDeleteBatch;
use MongoInsertBatch;
use MongoUpdateBatch;

abstract class AbstractGenerator implements Iterator
{
    const CONN_STANDALONE = 0x01;
    const CONN_PRIMARY = 0x02;
    const CONN_SECONDARY = 0x04;
    const CONN_ARBITER = 0x08;
    const CONN_MONGOS = 0x10;

    const PHP_MONGO_API_WRITE_API = 2;

    private $client;
    private $collection;
    private $db;
    private $writeOptions;

    protected $operations;
    protected $currentBatch;
    protected $currentBatchIndex = 0;

    /**
     * Constructor.
     *
     * @param MongoClient $client       MongoClient instance
     * @param string      $db           Database name
     * @param string      $collection   Collection name
     * @param array       $operations   Operation type/document tuples
     * @param array       $writeOptions Write concern and ordered options
     */
    public function __construct(MongoClient $client, $db, $collection, array $operations, array $writeOptions)
    {
        $this->client = $client;
        $this->db = $client->selectDB($db);
        $this->collection = $client->selectCollection($db, $collection);
        $this->operations = $operations;
        $this->writeOptions = $writeOptions;
    }

    /**
     * Return the current batch.
     *
     * @see Iterator::current()
     * @return BatchInterface|null
     */
    public function current()
    {
        $this->initCurrentBatch();

        return $this->currentBatch;
    }

    /**
     * Return the index of the current batch.
     *
     * @see Iterator::key()
     * @return integer
     */
    public function key()
    {
        return $this->currentBatchIndex;
    }

    /**
     * Advance to the next batch.
     *
     * @see Iterator::next()
     */
    public function next()
    {
        $this->currentBatch = null;
        $this->currentBatchIndex += 1;
    }

    /**
     * Rewind to the first batch.
     *
     * @see Iterator::rewind()
     */
    public function rewind()
    {
        $this->currentBatch = null;
        $this->currentBatchIndex = 0;
    }

    /**
     * Check if there is a current Batch after calls to rewind() or next().
     *
     * @see Iterator::valid()
     * @return boolean
     */
    public function valid()
    {
        $this->initCurrentBatch();

        return $this->currentBatch !== null;
    }

    /**
     * Create a LegacyWriteBatch or MongoWriteBatch instance for the operation
     * type, depending on whether the client connection supports write commands.
     *
     * @param integer $type
     * @return LegacyWriteBatch|MongoWriteBatch
     * @throws InvalidArgumentException if $type is invalid
     */
    final protected function createBatchForType($type)
    {
        switch ($type) {
            case BatchInterface::OP_INSERT:
                return $this->isWriteApiSupported()
                    ? new MongoInsertBatch($this->collection, $this->writeOptions)
                    : new LegacyInsertBatch($this->db, $this->collection, $this->writeOptions);

            case BatchInterface::OP_UPDATE:
                return $this->isWriteApiSupported()
                    ? new MongoUpdateBatch($this->collection, $this->writeOptions)
                    : new LegacyUpdateBatch($this->db, $this->collection, $this->writeOptions);

            case BatchInterface::OP_DELETE:
                return $this->isWriteApiSupported()
                    ? new MongoDeleteBatch($this->collection, $this->writeOptions)
                    : new LegacyDeleteBatch($this->db, $this->collection, $this->writeOptions);

            default:
                throw new InvalidArgumentException(sprintf('Invalid type: %d', $type));
        }
    }

    /**
     * Initialize the current batch.
     */
    abstract protected function initCurrentBatch();

    /**
     * Checks if the client's writable connection(s) support write commands.
     *
     * @return boolean
     */
    private function isWriteApiSupported()
    {
        $writable = (self::CONN_STANDALONE | self::CONN_PRIMARY | self::CONN_MONGOS);

        foreach ($this->client->getConnections() as $connection) {
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
