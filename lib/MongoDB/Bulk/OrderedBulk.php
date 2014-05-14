<?php

namespace MongoDB\Bulk;

use MongoDB\Batch\OrderedGenerator;
use Iterator;

final class OrderedBulk extends AbstractBulk
{
    /**
     * Return whether bulk operations are ordered.
     *
     * @see Bulk::isOrdered()
     * @return boolean
     */
    public function isOrdered()
    {
        return true;
    }

    /**
     * Get the batches to be executed.
     *
     * @see Bulk::getBatches()
     * @param array $writeConcern
     * @return Iterator of MappedBatch instances
     */
    protected function getMappedBatches(array $writeConcern = array())
    {
        return new OrderedGenerator(
            $this->client,
            $this->db,
            $this->collection,
            $this->getOperations(),
            $writeConcern
        );
    }
}
