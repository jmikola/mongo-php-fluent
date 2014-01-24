<?php

namespace MongoDB;

use InvalidArgumentException;
use OverflowException;

class Batch
{
    private $baseIndex;
    private $bsonSize = 0;
    private $documents = array();
    private $type;

    /**
     * Constructor.
     *
     * @param integer $type
     * @param integer $baseIndex
     * @throws InvalidArgumentException if $type is invalid or $baseIndex is negative
     */
    public function __construct($type, $baseIndex)
    {
        if ( ! in_array($type, array(BulkInterface::OP_INSERT, BulkInterface::OP_UPDATE, BulkInterface::OP_REMOVE))) {
            throw new InvalidArgumentException(sprintf('Invalid type: %d', $type));
        }

        if ($baseIndex < 0) {
            throw new InvalidArgumentException(sprintf('Base index cannot be negative: %d', $baseIndex));
        }

        $this->type = (integer) $type;
        $this->baseIndex = (integer) $baseIndex;
    }

    /**
     * Adds a document to the batch.
     *
     * The BSON size is taken as an argument to avoid recalculation.
     *
     * @param array|object $document
     * @param integer      $bsonSize
     * @throws OverflowException if the batch cannot accomodate the document due
     *                           to either the document or BSON limit
     */
    public function push($document, $bsonSize)
    {
        if (count($this->documents) >= BulkInterface::MAX_BATCH_SIZE_DOCS) {
            throw new OverflowException(sprintf('Batch size cannot exceed %d documents', BulkInterface::MAX_BATCH_SIZE_DOCS));
        }

        if ($this->bsonSize + $bsonSize > BulkInterface::MAX_BATCH_SIZE_BYTES) {
            throw new OverflowException(sprintf(
                'Batch BSON size cannot exceed %d bytes. Current size is %d bytes. Document is %s bytes.',
                BulkInterface::MAX_BATCH_SIZE_BYTES, $this->bsonSize, $bsonSize
            ));
        }

        $this->documents[] = $document;
        $this->bsonSize += $bsonSize;
    }

    /**
     * Get the base index for the batch.
     *
     * @return integer
     */
    public function getBaseIndex()
    {
        return $this->baseIndex;
    }

    /**
     * Get the total BSON size of documents in the batch.
     *
     * @return integer
     */
    public function getBsonSize()
    {
        return $this->bsonSize;
    }

    /**
     * Get the documents in the batch.
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
     * Get the batch type.
     *
     * @return integer
     */
    public function getType()
    {
        return $this->type;
    }
}
