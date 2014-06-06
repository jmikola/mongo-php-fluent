<?php

namespace MongoDB\Batch;

use MongoDB\Batch\Legacy\LegacyWriteBatch;
use MongoDB\Exception\UnexpectedTypeException;
use OutOfBoundsException;
use MongoException;

/**
 * This class composes a BatchInterface instance and also maintains a map of
 * bulk indexes to the corresponding batch index, which is needed to merge
 * multiple batch results into a single bulk result.
 */
final class MappedBatch implements BatchInterface
{
    private $batch;
    private $indexMap = array();

    /**
     * Constructor.
     *
     * @param BatchInterface $batch
     */
    public function __construct(BatchInterface $batch)
    {
        $this->batch = $batch;
    }

    /**
     * Adds an operation document to the batch.
     *
     * @param array|object $document
     * @param integer      $bulkIndex
     * @throws OutOfBoundsException if $bulkIndex is negative or the batch
     *                              already contains a document for $bulkIndex
     */
    public function add($document, $bulkIndex = 0)
    {
        $bulkIndex = (integer) $bulkIndex;

        if ($bulkIndex < 0) {
            throw new OutOfBoundsException(sprintf('Bulk index cannot be negative: %d', $bulkIndex));
        }

        if (isset($this->indexMap[$bulkIndex])) {
            throw new OutOfBoundsException(sprintf('Document already exists for bulk index: %d', $bulkIndex));
        }

        $this->indexMap[$bulkIndex] = count($this->documents);
        $this->batch->add($document);
    }

    /**
     * @see BatchInterface::execute()
     */
    public function execute(array $writeOptions = array())
    {
        if ($this->batch->getItemCount() == 0) {
            throw new MongoException('Cannot call execute() for an empty batch');
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
     * @see BatchInterface::getItemCount()
     */
    public function getItemCount()
    {
        return $this->batch->getItemCount();
    }

    /**
     * @see BatchInterface::getType()
     */
    public function getType()
    {
        return $this->batch->getType();
    }
}
