<?php

namespace MongoDB;

use MongoDB\Exception\UnexpectedTypeException;
use InvalidArgumentException;
use OutOfBoundsException;
use OverflowException;

class Batch
{
    /**
     * Combined BSON size of all documents in the batch.
     *
     * @var integer
     */
    private $bsonSize = 0;

    /**
     * Batch documents (e.g. operations).
     *
     * @var array
     */
    private $documents = array();

    /**
     * Map of document bulk indexes to their batch index.
     *
     * @var array
     */
    private $indexMap = array();

    /**
     * Operation type for batch documents.
     *
     * @var integer
     */
    private $type;

    /**
     * Constructor.
     *
     * @param integer $type
     * @throws InvalidArgumentException if $type is invalid
     */
    public function __construct($type)
    {
        if ( ! in_array($type, array(BulkInterface::OP_INSERT, BulkInterface::OP_UPDATE, BulkInterface::OP_REMOVE))) {
            throw new InvalidArgumentException(sprintf('Invalid type: %d', $type));
        }

        $this->type = (integer) $type;
    }

    /**
     * Adds a document to the batch.
     *
     * The BSON size is taken as an argument to avoid recalculation.
     *
     * @param integer $bulkIndex
     * @param object  $document
     * @param integer $bsonSize
     * @throws OutOfBoundsException if $bulkIndex is negative or the batch
     *                              already contains a document for $bulkIndex
     * @throws OverflowException if the batch cannot accomodate the document due
     *                           to either the batch size or BSON size limit
     * @throws UnexpectedTypeException if $document is not an object
     */
    public function add($bulkIndex, $document, $bsonSize)
    {
        $bulkIndex = (integer) $bulkIndex;

        if ($bulkIndex < 0) {
            throw new OutOfBoundsException(sprintf('Bulk index cannot be negative: %d', $bulkIndex));
        }

        if (isset($this->indexMap[$bulkIndex])) {
            throw new OutOfBoundsException(sprintf('Document already exists for bulk index: %d', $bulkIndex));
        }

        if ( ! is_object($document)) {
            throw new UnexpectedTypeException($document, 'object');
        }

        if (count($this->documents) >= BulkInterface::MAX_BATCH_SIZE_DOCS) {
            throw new OverflowException(sprintf('Batch size cannot exceed %d documents', BulkInterface::MAX_BATCH_SIZE_DOCS));
        }

        if ($this->bsonSize + $bsonSize > BulkInterface::MAX_BATCH_SIZE_BYTES) {
            throw new OverflowException(sprintf(
                'Batch BSON size cannot exceed %d bytes. Current size is %d bytes. Document is %s bytes.',
                BulkInterface::MAX_BATCH_SIZE_BYTES, $this->bsonSize, $bsonSize
            ));
        }

        $this->indexMap[$bulkIndex] = count($this->documents);
        $this->documents[] = $document;
        $this->bsonSize += $bsonSize;
    }

    /**
     * Get the total BSON size of all documents in the batch.
     *
     * @return integer
     */
    public function getBsonSize()
    {
        return $this->bsonSize;
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
     * Get a document by its batch index.
     *
     * @param integer $batchIndex
     * @return object
     * @throws OutOfBoundsException if the batch has no document for $batchIndex
     */
    public function getDocument($batchIndex)
    {
        if ( ! isset($this->documents[$batchIndex])) {
            throw new OutOfBoundsException(sprintf('No document exists for batch index: %d', $batchIndex));
        }

        return $this->documents[$batchIndex];
    }

    /**
     * Get all documents in the batch.
     *
     * @return array
     */
    public function getDocuments()
    {
        return $this->documents;
    }

    /**
     * Get the number of documents in the batch.
     *
     * @return integer
     */
    public function getSize()
    {
        return count($this->documents);
    }

    /**
     * Get the operation type.
     *
     * @return integer
     */
    public function getType()
    {
        return $this->type;
    }
}
