<?php

namespace MongoDB;

use InvalidArgumentException;

class UnorderedBulk extends Bulk
{
    private $batches = array();
    private $currentBatch;
    private $currentIndex = 0;

    /**
     * Adds a write operation.
     *
     * @see Bulk::addOperation()
     * @param integer      $type
     * @param array|object $document
     * @throws InvalidArgumentException if the document BSON size exceeds
     *                                  BulkInterface::MAX_BATCH_SIZE_BYTES
     */
    protected function addOperation($type, $document)
    {
        $docSize = strlen(bson_encode($document));

        if ($docSize >= BulkInterface::MAX_BATCH_SIZE_BYTES) {
            throw new InvalidArgumentException(sprintf(
                'Document BSON size (%d) exceeds maximum (%d)',
                $docSize, BulkInterface::MAX_BATCH_SIZE_BYTES
            ));
        }

        /* TODO: This is not optimized to re-order writes into as few batches as
         * possible. Doing so will require operations in a single batch to have
         * specific index offsets, since they may no longer be sequential.
         */
        if ($this->currentBatch === null ||
            $this->currentBatch->getType() !== $type ||
            $this->currentBatch->getSize() + 1 >= BulkInterface::MAX_BATCH_SIZE_DOCS ||
            $this->currentBatch->getBsonSize() + $docSize >= BulkInterface::MAX_BATCH_SIZE_BYTES) {

            $this->currentBatch = new Batch($type);
            $this->batches[] = $this->currentBatch;
        }

        $this->currentBatch->add($this->currentIndex, $document, $docSize);
        $this->currentIndex += 1;
    }

    /**
     * Get the batches to be executed.
     *
     * @see Bulk::getBatches()
     * @return array
     */
    protected function getBatches()
    {
        return $this->batches;
    }

    /**
     * Return whether bulk operations are ordered.
     *
     * @see Bulk::isOrdered()
     * @return boolean
     */
    protected function isOrdered()
    {
        return false;
    }
}
