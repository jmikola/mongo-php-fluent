<?php

namespace MongoDB\Batch;

use MongoDB\Batch\Legacy\LegacyDeleteBatch;
use MongoDB\Batch\Legacy\LegacyInsertBatch;
use MongoDB\Batch\Legacy\LegacyUpdateBatch;
use MongoDB\Batch\Legacy\LegacyWriteBatch;
use MongoDB\Exception\UnexpectedTypeException;
use BadMethodCallException;
use InvalidArgumentException;
use MongoDeleteBatch;
use MongoInsertBatch;
use MongoUpdateBatch;
use MongoWriteBatch;
use OutOfBoundsException;

/**
 * This class composes a LegacyWriteBatch or MongoWriteBatch instance and also
 * maintains a map of bulk indexes to the corresponding batch index, which is
 * needed to merge multiple batch results into a single bulk result.
 */
final class MappedBatch implements BatchInterface
{
    private $batch;
    private $documents = array();
    private $indexMap = array();
    private $type;

    /**
     * Constructor.
     *
     * @param LegacyWriteBatch|MongoWriteBatch $batch
     * @throws UnexpectedTypeException if $batch is not an object
     * @throws InvalidArgumentException if the batch type cannot be inferred
     */
    public function __construct($batch)
    {
        if ( ! is_object($batch)) {
            throw new UnexpectedTypeException($batch, 'object');
        }

        switch (true) {
            case $batch instanceof MongoInsertBatch:
            case $batch instanceof LegacyInsertBatch:
                $this->type = BatchInterface::OP_INSERT;
                break;

            case $batch instanceof MongoUpdateBatch:
            case $batch instanceof LegacyUpdateBatch:
                $this->type = BatchInterface::OP_UPDATE;
                break;

            case $batch instanceof MongoDeleteBatch:
            case $batch instanceof LegacyDeleteBatch:
                $this->type = BatchInterface::OP_DELETE;
                break;

            default:
                throw new InvalidArgumentException(sprintf('Could not infer type from class: %s', get_class($batch)));
        }

        $this->batch = $batch;
    }

    /**
     * Adds an operation document to the batch.
     *
     * @param object  $document
     * @param integer $bulkIndex
     * @throws OutOfBoundsException if $bulkIndex is negative or the batch
     *                              already contains a document for $bulkIndex
     * @throws UnexpectedTypeException if $document is not an object
     */
    public function add($document, $bulkIndex = 0)
    {
        if ( ! is_object($document)) {
            throw new UnexpectedTypeException($document, 'object');
        }

        $bulkIndex = (integer) $bulkIndex;

        if ($bulkIndex < 0) {
            throw new OutOfBoundsException(sprintf('Bulk index cannot be negative: %d', $bulkIndex));
        }

        if (isset($this->indexMap[$bulkIndex])) {
            throw new OutOfBoundsException(sprintf('Document already exists for bulk index: %d', $bulkIndex));
        }

        $this->indexMap[$bulkIndex] = count($this->documents);
        $this->documents[] = $document;
        $this->batch->add($document);
    }

    /**
     * Execute the batch.
     *
     * @param array $writeOptions Write concern and ordered options.
     * @return array
     * @throws BadMethodCallException if the batch is empty
     */
    public function execute(array $writeOptions = array())
    {
        if ($this->isEmpty()) {
            throw new BadMethodCallException('Cannot call execute() for an empty batch');
        }

        return $this->batch->execute($writeOptions);
    }

    /**
     * Get the bulk index for a document by its batch index.
     *
     * @param integer $batchIndex
     * @return integer
     * @throws OutOfBoundsException if the batch has no document for $batchIndex
     */
    public function getBulkIndex($batchIndex)
    {
        if (($bulkIndex = array_search($batchIndex, $this->indexMap)) === false) {
            throw new OutOfBoundsException(sprintf('No document exists for batch index: %d', $batchIndex));
        }

        return $bulkIndex;
    }

    /**
     * Get the type of operations in the batch.
     *
     * @return integer
     */
    public function getType()
    {
        return $this->type;
    }

    /**
     * Return whether the batch is empty and contains no operations.
     *
     * @return boolean
     */
    public function isEmpty()
    {
        return empty($this->documents);
    }
}
