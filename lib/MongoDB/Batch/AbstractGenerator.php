<?php

namespace MongoDB\Batch;

use MongoDB\Batch\Command\CommandDeleteBatch;
use MongoDB\Batch\Command\CommandInsertBatch;
use MongoDB\Batch\Command\CommandUpdateBatch;
use MongoDB\Batch\Legacy\LegacyDeleteBatch;
use MongoDB\Batch\Legacy\LegacyInsertBatch;
use MongoDB\Batch\Legacy\LegacyUpdateBatch;
use InvalidArgumentException;
use Iterator;
use MongoCollection;
use MongoDB;

abstract class AbstractGenerator implements Iterator
{
    const PHP_MONGO_API_WRITE_API = 2;

    private $collection;
    private $db;
    private $isWriteApiSupported;
    private $writeOptions;

    protected $operations;
    protected $currentBatch;
    protected $currentBatchIndex = 0;

    /**
     * Constructor.
     *
     * @param MongoDB         $db           MongoDB instance
     * @param MongoCollection $collection   MongoCollection instance
     * @param array           $operations   Operation type/document tuples
     * @param array           $writeOptions Write concern and ordered options
     */
    public function __construct(MongoDB $db, MongoCollection $collection, array $operations, array $writeOptions)
    {
        $this->db = $db;
        $this->collection = $collection;
        $this->operations = $operations;
        $this->writeOptions = $writeOptions;
    }

    /**
     * Return the current batch.
     *
     * @see Iterator::current()
     * @return MappedBatch|null
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
     * Create a BatchInterface instance for the operation type.
     *
     * The batch implementation returned will depend on whether or not the
     * connection supports write commands.
     *
     * @param integer $type
     * @return BatchInterface
     * @throws InvalidArgumentException if $type is invalid
     */
    final protected function createBatchForType($type)
    {
        switch ($type) {
            case BatchInterface::OP_INSERT:
                return $this->isWriteApiSupported()
                    ? new CommandInsertBatch($this->collection, $this->writeOptions)
                    : new LegacyInsertBatch($this->db, $this->collection, $this->writeOptions);

            case BatchInterface::OP_UPDATE:
                return $this->isWriteApiSupported()
                    ? new CommandUpdateBatch($this->collection, $this->writeOptions)
                    : new LegacyUpdateBatch($this->db, $this->collection, $this->writeOptions);

            case BatchInterface::OP_DELETE:
                return $this->isWriteApiSupported()
                    ? new CommandDeleteBatch($this->collection, $this->writeOptions)
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
        if (isset($this->isWriteApiSupported)) {
            return $this->isWriteApiSupported;
        }

        $this->isWriteApiSupported = false;

        $isMaster = $this->db->command(array('isMaster' => 1));

        if (isset($isMaster['minWireVersion']) && $isMaster['minWireVersion'] <= self::PHP_MONGO_API_WRITE_API &&
            isset($isMaster['maxWireVersion']) && $isMaster['maxWireVersion'] >= self::PHP_MONGO_API_WRITE_API) {

            $this->isWriteApiSupported = true;
        }

        return $this->isWriteApiSupported;
    }
}
